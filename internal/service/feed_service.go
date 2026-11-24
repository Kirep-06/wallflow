package service

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/repository"
)

const (
	maxBigVFanoutRead        = 200
	outboxRebuildConcurrency = 8
	feedRebuildTimeout       = 5 * time.Second
	hotFallbackKey           = "hot_top"
)

// HotFeedResult 封装热门榜读取结果与首屏弱 ETag / 304 协商状态。
type HotFeedResult struct {
	Page        *response.PageData[model.WallpaperSummary]
	ETag        string
	NotModified bool
}

// FeedServiceDeps 聚合关注流与热门榜读服务所需的仓储、缓存与装配依赖。
type FeedServiceDeps struct {
	FollowRepo        *repository.FollowRepository
	WallpaperRepo     *repository.WallpaperRepository
	FollowingIDsCache *cache.MultiLevel[[]uint64]
	BigVSet           *cache.BigVSet
	FollowingFeed     *cache.FollowingFeed
	HotRank           *cache.HotRank
	WallpaperSvc      *WallpaperService
}

// FeedService 实现推拉结合关注流与重力时间衰减热门榜（含首屏 ETag/304 与 singleflight 降级）。
type FeedService struct {
	followRepo        *repository.FollowRepository
	wallpaperRepo     *repository.WallpaperRepository
	followingIDsCache *cache.MultiLevel[[]uint64]
	bigVSet           *cache.BigVSet
	followingFeed     *cache.FollowingFeed
	hotRank           *cache.HotRank
	wallpaperSvc      *WallpaperService
	hotBatchLoader    func(ctx context.Context, beforeID uint64, limit int) ([]model.HotRankCandidate, error)

	inboxSF       singleflight.Group
	outboxSF      singleflight.Group
	hotFallbackSF singleflight.Group
}

// NewFeedService 构造信息流读服务。
func NewFeedService(deps FeedServiceDeps) *FeedService {
	var hotBatchLoader func(ctx context.Context, beforeID uint64, limit int) ([]model.HotRankCandidate, error)
	if deps.WallpaperRepo != nil {
		hotBatchLoader = deps.WallpaperRepo.ListHotRankBatch
	}
	return &FeedService{
		followRepo:        deps.FollowRepo,
		wallpaperRepo:     deps.WallpaperRepo,
		followingIDsCache: deps.FollowingIDsCache,
		bigVSet:           deps.BigVSet,
		followingFeed:     deps.FollowingFeed,
		hotRank:           deps.HotRank,
		wallpaperSvc:      deps.WallpaperSvc,
		hotBatchLoader:    hotBatchLoader,
	}
}

// ListHotFeed 返回全站热门壁纸榜 Top 100（可选鉴权、ScoreCursor 复合游标分页、首屏弱 ETag / 304 协商与 Redis 故障降级）。
func (s *FeedService) ListHotFeed(
	ctx context.Context,
	viewerID uint64,
	rawCursor string,
	rawLimit int,
	ifNoneMatch string,
) (*HotFeedResult, error) {
	limit, err := normalizeListLimit(rawLimit)
	if err != nil {
		return nil, err
	}
	scoreCur, err := parseScoreCursor(rawCursor)
	if err != nil {
		return nil, err
	}

	isFirstPage := scoreCur == nil
	var candidateVer string
	var matchedVers []string
	if isFirstPage {
		candidateVer, matchedVers = parseHotIfNoneMatch(ifNoneMatch, limit, viewerID)
	}

	var (
		entries []model.HotRankEntry
		etag    string
		fromHot bool
	)
	if s.hotRank != nil {
		snap, err := s.hotRank.ReadSnapshot(ctx, candidateVer)
		if err != nil {
			slog.WarnContext(ctx, "read hot rank snapshot degraded to mysql",
				slog.Uint64("viewer_id", viewerID),
				slog.Any("err", err),
			)
		} else if snap.Ready {
			fromHot = true
			if isFirstPage {
				etag = formatHotETag(snap.Version, limit, viewerID)
			}
			if etag != "" && (snap.NotModified || slices.Contains(matchedVers, snap.Version)) {
				return &HotFeedResult{ETag: etag, NotModified: true}, nil
			}
			entries = snap.Entries
		}
	}

	if !fromHot {
		entries, err = s.loadHotFallbackEntries(ctx)
		if err != nil {
			return nil, errno.ErrInternal.Wrap(err)
		}
	}

	var afterScore float64
	var afterID uint64
	if scoreCur != nil {
		afterScore = scoreCur.Score
		afterID = scoreCur.ID
	}
	pageEntries, hasMore := model.SliceHotPage(entries, afterScore, afterID, scoreCur != nil, limit)

	pageIDs := make([]uint64, len(pageEntries))
	hotScores := make(map[uint64]float64, len(pageEntries))
	for i, e := range pageEntries {
		pageIDs[i] = e.WallpaperID
		hotScores[e.WallpaperID] = e.Score
	}
	var nextCursor string
	if hasMore && len(pageEntries) > 0 {
		last := pageEntries[len(pageEntries)-1]
		nextCursor, err = cursor.Encode(cursor.ScoreCursor{Score: last.Score, ID: last.WallpaperID})
		if err != nil {
			return nil, errno.ErrInternal.Wrap(err)
		}
	}

	page, err := s.wallpaperSvc.assembleSummaryPage(ctx, pageIDs, nextCursor, hasMore, summaryAssembleOpts{
		viewerID:  viewerID,
		hotScores: hotScores,
	})
	if err != nil {
		return nil, err
	}
	return &HotFeedResult{Page: page, ETag: etag}, nil
}

func (s *FeedService) loadHotFallbackEntries(ctx context.Context) ([]model.HotRankEntry, error) {
	if s.hotBatchLoader == nil {
		return []model.HotRankEntry{}, nil
	}
	ch := s.hotFallbackSF.DoChan(hotFallbackKey, func() (any, error) {
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), feedRebuildTimeout)
		defer cancel()
		return model.ComputeHotTopN(loadCtx, s.hotBatchLoader, time.Now().UTC())
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		entries, ok := res.Val.([]model.HotRankEntry)
		if !ok {
			return nil, errors.New("service: unexpected hot rank fallback type")
		}
		return entries, nil
	}
}

func parseScoreCursor(raw string) (*cursor.ScoreCursor, error) {
	if raw == "" {
		return nil, nil
	}
	c, err := cursor.Decode[cursor.ScoreCursor](raw)
	if err != nil {
		return nil, err
	}
	if c.ID == 0 || math.IsNaN(c.Score) || math.IsInf(c.Score, 0) || c.Score <= 0 {
		return nil, cursor.ErrInvalid
	}
	return &c, nil
}

func formatHotETag(version string, limit int, viewerID uint64) string {
	if version == "" {
		return ""
	}
	return fmt.Sprintf(`W/"hot-%s-l%d-u%d"`, version, limit, viewerID)
}

func parseHotIfNoneMatch(header string, limit int, viewerID uint64) (string, []string) {
	trimmed := strings.TrimSpace(header)
	if trimmed == "" {
		return "", nil
	}
	if trimmed == "*" {
		return "*", []string{"*"}
	}

	suffix := fmt.Sprintf("-l%d-u%d", limit, viewerID)
	var matched []string
	for part := range strings.SplitSeq(trimmed, ",") {
		tag := strings.TrimSpace(part)
		if tag == "*" {
			return "*", []string{"*"}
		}
		tag = strings.TrimPrefix(tag, "W/")
		if len(tag) < 2 || tag[0] != '"' || tag[len(tag)-1] != '"' {
			continue
		}
		opaque := tag[1 : len(tag)-1]
		if !strings.HasPrefix(opaque, "hot-") || !strings.HasSuffix(opaque, suffix) {
			continue
		}
		if ver := opaque[len("hot-") : len(opaque)-len(suffix)]; ver != "" {
			matched = append(matched, ver)
		}
	}
	if len(matched) == 1 {
		return matched[0], matched
	}
	return "", matched
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

	rawIDs, err := s.fetchFollowingIDs(ctx, userID, followingIDs, beforeID, limit+1)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	pageIDs, nextCursor, hasMore, err := paginateIDs(rawIDs, limit)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	// 复用 M3 两段式 MultiLevel.MGet 装配，点亮计数与互动态并叠加有序关注集二分兜底过滤并发取关残余。
	return s.wallpaperSvc.assembleSummaryPage(ctx, pageIDs, nextCursor, hasMore, summaryAssembleOpts{
		viewerID: userID,
		allowAuthor: func(authorID uint64) bool {
			return containsSortedID(followingIDs, authorID)
		},
	})
}

// ListCategoryHotFeed 返回指定大分类（含子小类）或小分类内的 TopN 热门壁纸列表。
func (s *FeedService) ListCategoryHotFeed(ctx context.Context, viewerID, categoryID uint64, limit int) (*response.PageData[model.WallpaperSummary], error) {
	return s.wallpaperSvc.ListCategoryHot(ctx, viewerID, categoryID, limit)
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
	if s.followingFeed == nil || s.bigVSet == nil {
		return coldLoader(beforeID, want)
	}

	var bigVLoader func(context.Context) ([]uint64, error)
	if s.followRepo != nil {
		bigVLoader = s.followRepo.ListAllBigVIDs
	}
	bigVMap, err := s.bigVSet.Snapshot(ctx, bigVLoader)
	if err != nil {
		slog.WarnContext(ctx, "load bigv snapshot degraded to mysql following query",
			slog.Uint64("user_id", userID),
			slog.Any("err", err),
		)
		return coldLoader(beforeID, want)
	}

	var normalAuthors, bigVAuthors []uint64
	for _, uid := range followingIDs {
		if _, ok := bigVMap[uid]; ok {
			bigVAuthors = append(bigVAuthors, uid)
		} else {
			normalAuthors = append(normalAuthors, uid)
		}
	}
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

type feedStream struct {
	ids     []uint64
	floorID uint64
}

// loadHotStreams 拉取并按需 singleflight 懒重建普通作者收件箱与大 V 发件箱；
// 返回 ok=false 表示 Redis 底层故障，调用方降级为全量 MySQL 查询。
func (s *FeedService) loadHotStreams(
	ctx context.Context,
	userID uint64,
	normalAuthors, bigVAuthors []uint64,
	beforeID uint64,
	want int,
) ([]feedStream, bool, error) {
	streams := make([]feedStream, 0, 1+len(bigVAuthors))

	if len(normalAuthors) > 0 {
		inboxRes, err := s.followingFeed.ReadInbox(ctx, userID, beforeID, want)
		if err != nil {
			slog.WarnContext(ctx, "read following inbox degraded to mysql", slog.Uint64("user_id", userID), slog.Any("err", err))
			return nil, false, nil
		}
		if inboxRes.Exists {
			streams = append(streams, feedStream{ids: inboxRes.IDs, floorID: inboxRes.FloorID})
		} else {
			rebuilt, err := s.singleflightIDs(ctx, &s.inboxSF, userID, func(loadCtx context.Context) ([]uint64, error) {
				ids, err := s.wallpaperRepo.ListAuthorsWallpaperIDs(loadCtx, normalAuthors, 0, cache.InboxMaxSize)
				if err != nil {
					return nil, err
				}
				if err := s.followingFeed.RebuildInbox(loadCtx, userID, ids); err != nil {
					slog.WarnContext(loadCtx, "rebuild following inbox cache failed", slog.Uint64("user_id", userID), slog.Any("err", err))
				}
				return ids, nil
			})
			if err != nil {
				return nil, false, err
			}
			streams = append(streams, sliceRebuiltBox(rebuilt, beforeID, want, cache.InboxMaxSize))
		}
	}

	if len(bigVAuthors) > 0 {
		outboxes, err := s.followingFeed.ReadOutboxes(ctx, bigVAuthors, beforeID, want)
		if err != nil {
			slog.WarnContext(ctx, "read bigv outboxes degraded to mysql", slog.Uint64("user_id", userID), slog.Int("bigv_count", len(bigVAuthors)), slog.Any("err", err))
			return nil, false, nil
		}

		var missAuthors []uint64
		for _, aid := range bigVAuthors {
			if box := outboxes[aid]; box.Exists {
				streams = append(streams, feedStream{ids: box.IDs, floorID: box.FloorID})
			} else {
				missAuthors = append(missAuthors, aid)
			}
		}

		if len(missAuthors) > 0 {
			var mu sync.Mutex
			g, gctx := errgroup.WithContext(ctx)
			g.SetLimit(outboxRebuildConcurrency)
			for _, aid := range missAuthors {
				g.Go(func() error {
					ids, err := s.singleflightIDs(gctx, &s.outboxSF, aid, func(loadCtx context.Context) ([]uint64, error) {
						loaded, err := s.wallpaperRepo.ListUserWallpaperIDs(loadCtx, aid, 0, cache.OutboxMaxSize)
						if err != nil {
							return nil, err
						}
						if err := s.followingFeed.RebuildOutbox(loadCtx, aid, loaded); err != nil {
							slog.WarnContext(loadCtx, "rebuild bigv outbox cache failed", slog.Uint64("author_id", aid), slog.Any("err", err))
						}
						return loaded, nil
					})
					if err != nil {
						return err
					}
					st := sliceRebuiltBox(ids, beforeID, want, cache.OutboxMaxSize)
					mu.Lock()
					streams = append(streams, st)
					mu.Unlock()
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				return nil, false, err
			}
		}
	}

	return streams, true, nil
}

func (s *FeedService) singleflightIDs(
	ctx context.Context,
	sf *singleflight.Group,
	id uint64,
	load func(context.Context) ([]uint64, error),
) ([]uint64, error) {
	if s.wallpaperRepo == nil {
		return []uint64{}, nil
	}
	ch := sf.DoChan(strconv.FormatUint(id, 10), func() (any, error) {
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), feedRebuildTimeout)
		defer cancel()
		return load(loadCtx)
	})
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
		safeWatermark = max(safeWatermark, st.floorID)
		if len(st.ids) > 0 {
			idLists = append(idLists, st.ids)
		}
	}

	merged := mergeDescendingKWay(idLists, want)
	if safeWatermark == 0 || (len(merged) >= want && merged[want-1] >= safeWatermark) {
		return merged, nil
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
	return append(safeHot, coldIDs...), nil
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

	h := make(descMergeHeap, 0, len(nonEmpty))
	for idx, s := range nonEmpty {
		h = append(h, descMergeNode{id: s[0], streamIdx: idx})
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
		if nextIdx := top.elemIdx + 1; nextIdx < len(nonEmpty[top.streamIdx]) {
			h[0] = descMergeNode{id: nonEmpty[top.streamIdx][nextIdx], streamIdx: top.streamIdx, elemIdx: nextIdx}
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
func (h descMergeHeap) Less(i, j int) bool { return h[i].id > h[j].id }
func (h descMergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *descMergeHeap) Push(x any)        { *h = append(*h, x.(descMergeNode)) }
func (h *descMergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
