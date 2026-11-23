package service

import (
	"container/heap"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/repository"
)

const (
	maxBigVFanoutRead        = 200
	outboxRebuildConcurrency = 8
	feedRebuildTimeout       = 5 * time.Second
)

// FeedServiceDeps 聚合关注流读服务所需的全部仓储、缓存与装配依赖。
type FeedServiceDeps struct {
	FollowRepo        *repository.FollowRepository
	WallpaperRepo     *repository.WallpaperRepository
	FollowingIDsCache *cache.MultiLevel[[]uint64]
	BigVSet           *cache.BigVSet
	FollowingFeed     *cache.FollowingFeed
	WallpaperSvc      *WallpaperService
}

// FeedService 实现推拉结合关注流：收/发件箱懒重建、最大堆 K 路归并去重与全局安全水位线无缝冷补查。
type FeedService struct {
	followRepo        *repository.FollowRepository
	wallpaperRepo     *repository.WallpaperRepository
	followingIDsCache *cache.MultiLevel[[]uint64]
	bigVSet           *cache.BigVSet
	followingFeed     *cache.FollowingFeed
	wallpaperSvc      *WallpaperService

	inboxSF  singleflight.Group
	outboxSF singleflight.Group
}

// NewFeedService 构造推拉结合关注流读服务。
func NewFeedService(deps FeedServiceDeps) *FeedService {
	return &FeedService{
		followRepo:        deps.FollowRepo,
		wallpaperRepo:     deps.WallpaperRepo,
		followingIDsCache: deps.FollowingIDsCache,
		bigVSet:           deps.BigVSet,
		followingFeed:     deps.FollowingFeed,
		wallpaperSvc:      deps.WallpaperSvc,
	}
}

// ListFollowingFeed 返回当前登录用户的关注流瀑布流（强制鉴权，按 wallpaper.id 倒序游标分页）。
func (s *FeedService) ListFollowingFeed(
	ctx context.Context,
	userID uint64,
	rawCursor string,
	rawLimit int,
) (*response.PageData[model.WallpaperSummary], error) {
	limit, err := normalizeListLimit(rawLimit)
	if err != nil {
		return nil, err
	}
	beforeID, err := parseIDCursor(rawCursor)
	if err != nil {
		return nil, err
	}

	followingIDs, err := getFollowingIDs(ctx, s.followingIDsCache, s.followRepo, userID)
	if err != nil {
		return nil, err
	}
	if len(followingIDs) == 0 {
		empty := response.NewPageData[model.WallpaperSummary](nil, "", false)
		return &empty, nil
	}

	want := limit + 1
	rawIDs, err := s.fetchFollowingIDs(ctx, userID, followingIDs, beforeID, want)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	pageIDs, nextCursor, hasMore, err := paginateIDs(rawIDs, limit)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	// 复用 M3 两段式 MultiLevel.MGet 装配，并叠加有序关注集二分兜底过滤并发取关残余。
	return s.wallpaperSvc.assembleSummaryPage(ctx, pageIDs, nextCursor, hasMore, nil, func(authorID uint64) bool {
		return containsSortedID(followingIDs, authorID)
	})
}

func (s *FeedService) fetchFollowingIDs(
	ctx context.Context,
	userID uint64,
	followingIDs []uint64,
	beforeID uint64,
	want int,
) ([]uint64, error) {
	coldLoader := func(coldBefore uint64, count int) ([]uint64, error) {
		if s.wallpaperRepo == nil || count <= 0 {
			return []uint64{}, nil
		}
		return s.wallpaperRepo.ListAuthorsWallpaperIDs(ctx, followingIDs, coldBefore, count)
	}

	if s.followingFeed == nil || !s.followingFeed.Available() {
		return coldLoader(beforeID, want)
	}

	bigVMap, err := s.loadBigVSnapshot(ctx)
	if err != nil {
		slog.WarnContext(ctx, "load bigv snapshot degraded to mysql following query",
			slog.Uint64("user_id", userID),
			slog.Any("err", err),
		)
		return coldLoader(beforeID, want)
	}

	normalAuthors, bigVAuthors := splitAuthorsByBigV(followingIDs, bigVMap)
	if len(bigVAuthors) > maxBigVFanoutRead {
		return coldLoader(beforeID, want)
	}

	streams, ok, err := s.loadHotStreams(ctx, userID, normalAuthors, bigVAuthors, beforeID, want)
	if err != nil {
		return nil, err
	}
	if !ok {
		return coldLoader(beforeID, want)
	}

	return resolveFollowingFeedIDs(streams, beforeID, want, coldLoader)
}

func (s *FeedService) loadBigVSnapshot(ctx context.Context) (map[uint64]struct{}, error) {
	if s.bigVSet == nil {
		if s.followRepo == nil {
			return map[uint64]struct{}{}, nil
		}
		ids, err := s.followRepo.ListAllBigVIDs(ctx)
		if err != nil {
			return nil, err
		}
		out := make(map[uint64]struct{}, len(ids))
		for _, id := range ids {
			out[id] = struct{}{}
		}
		return out, nil
	}

	var loader func(context.Context) ([]uint64, error)
	if s.followRepo != nil {
		loader = s.followRepo.ListAllBigVIDs
	}
	return s.bigVSet.Snapshot(ctx, loader)
}

type feedStream struct {
	ids     []uint64
	floorID uint64
}

// loadHotStreams 拉取并按需 singleflight 懒重建普通作者收件箱与各大 V 发件箱。
// 返回 ok=false 表示 Redis 底层故障，调用方应透明降级为全量 MySQL 查询。
func (s *FeedService) loadHotStreams(
	ctx context.Context,
	userID uint64,
	normalAuthors, bigVAuthors []uint64,
	beforeID uint64,
	want int,
) ([]feedStream, bool, error) {
	streams := make([]feedStream, 0, 1+len(bigVAuthors))

	// 1. 普通作者收件箱（冷用户走 singleflight 懒重建）
	if len(normalAuthors) > 0 {
		inboxRes, err := s.followingFeed.ReadInbox(ctx, userID, beforeID, want)
		if err != nil {
			slog.WarnContext(ctx, "read following inbox degraded to mysql",
				slog.Uint64("user_id", userID),
				slog.Any("err", err),
			)
			return nil, false, nil
		}
		if inboxRes.Exists {
			streams = append(streams, feedStream{ids: inboxRes.IDs, floorID: inboxRes.FloorID})
		} else {
			rebuilt, err := s.rebuildInboxSingleflight(ctx, userID, normalAuthors)
			if err != nil {
				return nil, false, err
			}
			streams = append(streams, sliceRebuiltBox(rebuilt, beforeID, want, s.followingFeed.InboxMax()))
		}
	}

	// 2. 大 V 发件箱（单次 Pipeline 批量读 + 未命中项有界并发 singleflight 懒重建）
	if len(bigVAuthors) > 0 {
		outboxes, err := s.followingFeed.ReadOutboxes(ctx, bigVAuthors, beforeID, want)
		if err != nil {
			slog.WarnContext(ctx, "read bigv outboxes degraded to mysql",
				slog.Uint64("user_id", userID),
				slog.Int("bigv_count", len(bigVAuthors)),
				slog.Any("err", err),
			)
			return nil, false, nil
		}

		var missAuthors []uint64
		for _, aid := range bigVAuthors {
			box := outboxes[aid]
			if box.Exists {
				streams = append(streams, feedStream{ids: box.IDs, floorID: box.FloorID})
			} else {
				missAuthors = append(missAuthors, aid)
			}
		}

		if len(missAuthors) > 0 {
			rebuiltStreams, err := s.rebuildOutboxesBounded(ctx, missAuthors, beforeID, want)
			if err != nil {
				return nil, false, err
			}
			streams = append(streams, rebuiltStreams...)
		}
	}

	return streams, true, nil
}

func (s *FeedService) rebuildInboxSingleflight(ctx context.Context, userID uint64, normalAuthors []uint64) ([]uint64, error) {
	if s.wallpaperRepo == nil {
		return []uint64{}, nil
	}
	key := strconv.FormatUint(userID, 10)
	ch := s.inboxSF.DoChan(key, func() (any, error) {
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), feedRebuildTimeout)
		defer cancel()

		ids, err := s.wallpaperRepo.ListAuthorsWallpaperIDs(loadCtx, normalAuthors, 0, s.followingFeed.InboxMax())
		if err != nil {
			return nil, err
		}
		if err := s.followingFeed.RebuildInbox(loadCtx, userID, ids); err != nil {
			slog.WarnContext(loadCtx, "rebuild following inbox cache failed",
				slog.Uint64("user_id", userID),
				slog.Any("err", err),
			)
		}
		return ids, nil
	})
	return waitUint64SliceSF(ctx, ch)
}

func (s *FeedService) rebuildOutboxesBounded(
	ctx context.Context,
	authorIDs []uint64,
	beforeID uint64,
	want int,
) ([]feedStream, error) {
	outboxMax := s.followingFeed.OutboxMax()
	var (
		mu      sync.Mutex
		rebuilt = make([]feedStream, 0, len(authorIDs))
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(outboxRebuildConcurrency)

	for _, aid := range authorIDs {
		g.Go(func() error {
			ids, err := s.rebuildOutboxSingleflight(gctx, aid, outboxMax)
			if err != nil {
				return err
			}
			st := sliceRebuiltBox(ids, beforeID, want, outboxMax)
			mu.Lock()
			rebuilt = append(rebuilt, st)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return rebuilt, nil
}

func (s *FeedService) rebuildOutboxSingleflight(ctx context.Context, authorID uint64, maxSize int) ([]uint64, error) {
	if s.wallpaperRepo == nil {
		return []uint64{}, nil
	}
	key := strconv.FormatUint(authorID, 10)
	ch := s.outboxSF.DoChan(key, func() (any, error) {
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), feedRebuildTimeout)
		defer cancel()

		ids, err := s.wallpaperRepo.ListUserWallpaperIDs(loadCtx, authorID, 0, maxSize)
		if err != nil {
			return nil, err
		}
		if err := s.followingFeed.RebuildOutbox(loadCtx, authorID, ids); err != nil {
			slog.WarnContext(loadCtx, "rebuild bigv outbox cache failed",
				slog.Uint64("author_id", authorID),
				slog.Any("err", err),
			)
		}
		return ids, nil
	})
	return waitUint64SliceSF(ctx, ch)
}

func waitUint64SliceSF(ctx context.Context, ch <-chan singleflight.Result) ([]uint64, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		ids, ok := res.Val.([]uint64)
		if !ok {
			return nil, errors.New("service: unexpected singleflight slice type")
		}
		return ids, nil
	}
}

func splitAuthorsByBigV(followingIDs []uint64, bigVMap map[uint64]struct{}) (normal, bigV []uint64) {
	for _, uid := range followingIDs {
		if _, ok := bigVMap[uid]; ok {
			bigV = append(bigV, uid)
		} else {
			normal = append(normal, uid)
		}
	}
	return normal, bigV
}

func sliceRebuiltBox(descIDs []uint64, beforeID uint64, want, maxSize int) feedStream {
	var floorID uint64
	if len(descIDs) >= maxSize && len(descIDs) > 0 {
		floorID = descIDs[len(descIDs)-1]
	}

	filtered := make([]uint64, 0, min(len(descIDs), want))
	for _, id := range descIDs {
		if beforeID > 0 && id >= beforeID {
			continue
		}
		filtered = append(filtered, id)
		if len(filtered) >= want {
			break
		}
	}
	return feedStream{ids: filtered, floorID: floorID}
}

// resolveFollowingFeedIDs 执行最大堆 K 路归并去重，并基于全局安全水位线 W_safe 决定是否无缝回源 MySQL 冷补查。
func resolveFollowingFeedIDs(
	streams []feedStream,
	beforeID uint64,
	want int,
	coldLoader func(coldBefore uint64, count int) ([]uint64, error),
) ([]uint64, error) {
	if want <= 0 {
		return []uint64{}, nil
	}

	var safeWatermark uint64
	idLists := make([][]uint64, 0, len(streams))
	for _, st := range streams {
		if st.floorID > safeWatermark {
			safeWatermark = st.floorID
		}
		if len(st.ids) > 0 {
			idLists = append(idLists, st.ids)
		}
	}

	merged := mergeDescendingKWay(idLists, want)
	if safeWatermark == 0 {
		return merged, nil
	}
	if len(merged) >= want && merged[want-1] >= safeWatermark {
		return merged[:want], nil
	}

	// 保留严格 >= W_safe 的安全热区前缀（100% 零漏帖），缺口无缝衔接 MySQL 冷补查（< coldBefore）。
	safeHot := make([]uint64, 0, len(merged))
	for _, id := range merged {
		if id < safeWatermark {
			break
		}
		safeHot = append(safeHot, id)
	}

	need := want - len(safeHot)
	if need <= 0 || coldLoader == nil {
		return safeHot, nil
	}

	coldBefore := beforeID
	if len(safeHot) > 0 {
		coldBefore = safeHot[len(safeHot)-1]
	}
	coldIDs, err := coldLoader(coldBefore, need)
	if err != nil {
		return nil, err
	}
	if len(safeHot) == 0 {
		return coldIDs, nil
	}
	out := make([]uint64, 0, len(safeHot)+len(coldIDs))
	out = append(out, safeHot...)
	out = append(out, coldIDs...)
	return out, nil
}

// mergeDescendingKWay 使用最大堆对多路降序壁纸 ID 执行 K 路归并，并通过单调尾值零分配去重。
func mergeDescendingKWay(streams [][]uint64, want int) []uint64 {
	if want <= 0 || len(streams) == 0 {
		return []uint64{}
	}

	nonEmpty := make([][]uint64, 0, len(streams))
	for _, s := range streams {
		if len(s) > 0 {
			nonEmpty = append(nonEmpty, s)
		}
	}
	if len(nonEmpty) == 0 {
		return []uint64{}
	}
	if len(nonEmpty) == 1 {
		src := nonEmpty[0]
		out := make([]uint64, 0, min(len(src), want))
		var last uint64
		for _, id := range src {
			if id == 0 || id == last {
				continue
			}
			out = append(out, id)
			last = id
			if len(out) >= want {
				break
			}
		}
		return out
	}

	h := make(descMergeHeap, 0, len(nonEmpty))
	for idx, s := range nonEmpty {
		h = append(h, descMergeNode{
			id:        s[0],
			streamIdx: idx,
			elemIdx:   0,
		})
	}
	heap.Init(&h)

	out := make([]uint64, 0, want)
	var lastEmitted uint64
	for len(h) > 0 && len(out) < want {
		top := h[0]
		if top.id > 0 && top.id != lastEmitted {
			out = append(out, top.id)
			lastEmitted = top.id
		}

		nextIdx := top.elemIdx + 1
		if nextIdx < len(nonEmpty[top.streamIdx]) {
			h[0] = descMergeNode{
				id:        nonEmpty[top.streamIdx][nextIdx],
				streamIdx: top.streamIdx,
				elemIdx:   nextIdx,
			}
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}
	return out
}

type descMergeNode struct {
	id        uint64
	streamIdx int
	elemIdx   int
}

type descMergeHeap []descMergeNode

func (h descMergeHeap) Len() int           { return len(h) }
func (h descMergeHeap) Less(i, j int) bool { return h[i].id > h[j].id } // 最大堆
func (h descMergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *descMergeHeap) Push(x any) {
	*h = append(*h, x.(descMergeNode))
}

func (h *descMergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
