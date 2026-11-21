package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
)

const (
	followingLockStripes  = 256
	outboxRebuildLimit    = 4
	coldBackfillLimit     = 4
	sfRebuildTimeout      = 5 * time.Second
	bigVLoadTimeout       = 5 * time.Second
	followingCleanTimeout = 2 * time.Second
)

type bigVSnapshot struct {
	ids       map[uint64]struct{}
	expiresAt time.Time
}

// FollowingFeedService 编排关注流推拉结合读路径、全站大 V 进程快照、256 分段锁与关注变更缓存失效。
type FollowingFeedService struct {
	postStore         PostStore
	followStore       FollowStore
	feedRepo          *repository.FollowingFeedRepo
	posts             *PostService
	followingIDsCache *cache.MultiLevel
	feedCfg           config.FeedConfig
	cfg               config.FollowingFeedConfig

	userLocks         [followingLockStripes]sync.Mutex
	outboxSF          singleflight.Group
	bigVSF            singleflight.Group
	bigVSnap          atomic.Pointer[bigVSnapshot]
	invalidateTimeout time.Duration
}

func NewFollowingFeedService(
	postStore PostStore,
	followStore FollowStore,
	feedRepo *repository.FollowingFeedRepo,
	posts *PostService,
	followingIDsCache *cache.MultiLevel,
	feedCfg config.FeedConfig,
	cfg config.FollowingFeedConfig,
) *FollowingFeedService {
	return &FollowingFeedService{
		postStore:         postStore,
		followStore:       followStore,
		feedRepo:          feedRepo,
		posts:             posts,
		followingIDsCache: followingIDsCache,
		feedCfg:           feedCfg,
		cfg:               cfg,
		invalidateTimeout: followingCleanTimeout,
	}
}

func (s *FollowingFeedService) PageMax() int { return s.feedCfg.PageMax }

func (s *FollowingFeedService) userLock(userID uint64) *sync.Mutex {
	return &s.userLocks[userID%followingLockStripes]
}

// OnFollowChanged 实现 FollowChangeHook：在关注/取关事务提交后联动更新全站大 V 集合与关注流缓存。
// 缓存写/删失败仅记 Warn 日志，绝不影响关注/取关主接口返回成功。
func (s *FollowingFeedService) OnFollowChanged(ctx context.Context, followerID, followeeID uint64, followeeIsBigV, promotedBigV bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	baseCtx := context.WithoutCancel(ctx)
	timeout := s.invalidateTimeout
	if timeout <= 0 {
		timeout = followingCleanTimeout
	}

	if promotedBigV {
		// 立即清空本进程 L1 大 V 快照，并尽力同步到 Redis 全站大 V 集合（Worker 60s 对账兜底）。
		s.bigVSnap.Store(nil)
		bigVCtx, cancel := context.WithTimeout(baseCtx, timeout)
		if _, err := s.feedRepo.AddBigVIfExists(bigVCtx, followeeID); err != nil {
			slog.WarnContext(bigVCtx, "add promoted bigv to redis failed", "author_id", followeeID, "err", err)
		}
		cancel()
	}

	if followerID == 0 {
		return
	}

	mu := s.userLock(followerID)
	mu.Lock()
	defer mu.Unlock()

	idsCtx, cancelIDs := context.WithTimeout(baseCtx, timeout)
	if err := s.followingIDsCache.Delete(idsCtx, followerID); err != nil {
		slog.WarnContext(idsCtx, "delete following_ids cache failed", "user_id", followerID, "err", err)
	}
	cancelIDs()

	// 普通作者或本次刚晋升大 V 时删除收件箱；老牌大 V 帖子本不在 inbox 中，保留收件箱减少无效重建。
	if !followeeIsBigV || promotedBigV {
		inboxCtx, cancelInbox := context.WithTimeout(baseCtx, timeout)
		if err := s.feedRepo.DeleteInbox(inboxCtx, followerID); err != nil {
			slog.WarnContext(inboxCtx, "delete following inbox failed", "user_id", followerID, "err", err)
		}
		cancelInbox()
	}
}

// Following 返回当前登录用户的关注流游标分页数据（严格不含用户自身发布的帖子）。
func (s *FollowingFeedService) Following(ctx context.Context, userID uint64, cursorStr string, limit int) (*model.FeedPage, error) {
	if userID == 0 {
		return nil, errno.ErrUnauth
	}
	before, err := cursor.DecodeID(cursorStr)
	if err != nil {
		return nil, errno.ErrBadCursor
	}
	if limit == 0 {
		limit = s.feedCfg.PageDefault
	}
	if limit < 1 || limit > s.feedCfg.PageMax {
		return nil, errno.ErrParam
	}

	followeeIDs, err := s.getFollowingIDs(ctx, userID)
	if err != nil {
		return nil, err
	}
	followeeIDs, followeeSet := normalizeFollowees(followeeIDs, userID)
	if len(followeeIDs) == 0 {
		return &model.FeedPage{
			Items:      []*model.PostDTO{},
			NextCursor: "",
			HasMore:    false,
		}, nil
	}

	bigVSet, err := s.getBigVSet(ctx)
	if err != nil {
		return nil, err
	}
	normalAuthors, bigVAuthors := splitAuthorsByBigV(followeeIDs, bigVSet)

	want := limit + 1
	ids, refreshedFollowees, err := s.collectPageIDs(ctx, userID, followeeIDs, normalAuthors, bigVAuthors, bigVSet, before, want)
	if err != nil {
		return nil, err
	}
	if refreshedFollowees != nil {
		followeeSet = refreshedFollowees
	}

	hasMore := len(ids) > limit
	if hasMore {
		ids = ids[:limit]
	}
	nextCursor := ""
	if hasMore && len(ids) > 0 {
		nextCursor = cursor.EncodeID(ids[len(ids)-1])
	}

	rawItems, err := s.posts.GetPosts(ctx, ids)
	if err != nil {
		return nil, err
	}
	// 水合后按最新关注集合二次过滤，杜绝极端并发下展示已取关作者或本人的帖子。
	items := make([]*model.PostDTO, 0, len(rawItems))
	for _, item := range rawItems {
		if item.UserID == userID {
			continue
		}
		if _, ok := followeeSet[item.UserID]; !ok {
			continue
		}
		items = append(items, item)
	}
	return &model.FeedPage{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// collectPageIDs 实现推拉混合读路径、分路安全水位线与精准单路冷补查，Redis 故障时透明降级回源 MySQL。
func (s *FollowingFeedService) collectPageIDs(
	ctx context.Context,
	userID uint64,
	allFollowees []uint64,
	normalAuthors []uint64,
	bigVAuthors []uint64,
	bigVSet map[uint64]struct{},
	before uint64,
	want int,
) ([]uint64, map[uint64]struct{}, error) {
	readInbox := len(normalAuthors) > 0
	inboxRes, outboxRes, err := s.feedRepo.RangeInboxAndOutboxes(ctx, userID, readInbox, bigVAuthors, before, want)
	if err != nil {
		slog.WarnContext(ctx, "following feed redis unavailable, fallback to mysql", "user_id", userID, "err", err)
		ids, dbErr := s.postStore.ListAuthorsIDsBefore(ctx, allFollowees, before, want, s.cfg.AuthorInChunkSize)
		return ids, nil, dbErr
	}

	// 检查缺失的大 V 发件箱数量，超过阈值则整体降级走单次分块 MySQL 查询，避免冷启动尾延迟放大。
	var missingBigVs []uint64
	for _, bid := range bigVAuthors {
		if res, ok := outboxRes[bid]; !ok || !res.Exists {
			missingBigVs = append(missingBigVs, bid)
		}
	}
	if len(missingBigVs) > s.cfg.MaxSyncOutboxRebuilds {
		slog.InfoContext(ctx, "too many missing bigv outboxes, fallback to mysql",
			"user_id", userID, "missing_count", len(missingBigVs))
		ids, dbErr := s.postStore.ListAuthorsIDsBefore(ctx, allFollowees, before, want, s.cfg.AuthorInChunkSize)
		return ids, nil, dbErr
	}

	buckets := make([]TimelineBucket, 0, len(bigVAuthors)+1)
	var refreshedFollowees map[uint64]struct{}
	effectiveNormals := normalAuthors

	// 1. 处理普通作者收件箱（inbox）
	if readInbox {
		if inboxRes != nil && inboxRes.Exists {
			buckets = append(buckets, TimelineBucket{
				AuthorID:  0,
				IDs:       inboxRes.IDs,
				SafeFloor: ComputeSafeFloor(true, inboxRes.Card, s.feedRepo.InboxHotSize(), inboxRes.MinID),
			})
		} else if before == 0 {
			bucket, newNormals, newFollowees, err := s.rebuildInboxAndSlice(ctx, userID, bigVSet, want)
			if err != nil {
				return nil, nil, err
			}
			if newFollowees != nil {
				refreshedFollowees = newFollowees
				effectiveNormals = newNormals
			}
			if len(bucket.IDs) > 0 || bucket.SafeFloor > 0 {
				buckets = append(buckets, bucket)
			}
		} else {
			// 深页请求且收件箱不存在：不强迫深页扛 500 条顶部重建，直接查本页冷数据（safeFloor = 0）。
			coldIDs, err := s.postStore.ListAuthorsIDsBefore(ctx, normalAuthors, before, want, s.cfg.AuthorInChunkSize)
			if err != nil {
				return nil, nil, err
			}
			buckets = append(buckets, TimelineBucket{
				AuthorID:  0,
				IDs:       coldIDs,
				SafeFloor: 0,
			})
		}
	}

	// 2. 收集已命中的大 V 发件箱
	for _, bid := range bigVAuthors {
		if res, ok := outboxRes[bid]; ok && res.Exists {
			buckets = append(buckets, TimelineBucket{
				AuthorID:  bid,
				IDs:       res.IDs,
				SafeFloor: ComputeSafeFloor(true, res.Card, s.feedRepo.OutboxHotSize(), res.MinID),
			})
		}
	}

	// 3. 有界并发 + singleflight 重建缺失的大 V 发件箱
	if len(missingBigVs) > 0 {
		rebuilt := make([]TimelineBucket, len(missingBigVs))
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(outboxRebuildLimit)
		for i, bid := range missingBigVs {
			g.Go(func() error {
				hotIDs, err := s.rebuildOutbox(gctx, bid)
				if err != nil {
					return err
				}
				var safeFloor uint64
				if len(hotIDs) >= s.feedRepo.OutboxHotSize() {
					safeFloor = hotIDs[s.feedRepo.OutboxHotSize()-1]
				}
				rebuilt[i] = TimelineBucket{
					AuthorID:  bid,
					IDs:       filterBeforeLimit(hotIDs, before, want),
					SafeFloor: safeFloor,
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, nil, err
		}
		buckets = append(buckets, rebuilt...)
	}

	// 4. 初步归并与精准单路冷补查
	cand, plan := MergeAndPlanColdBackfill(buckets, before, want)
	if plan.Empty() {
		if len(cand) > want {
			cand = cand[:want]
		}
		return cand, refreshedFollowees, nil
	}

	extraCount := len(plan.OutboxTargets)
	if plan.NeedInbox {
		extraCount++
	}
	extraLists := make([][]uint64, extraCount)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(coldBackfillLimit)
	idx := 0
	for _, target := range plan.OutboxTargets {
		slot := idx
		idx++
		t := target
		g.Go(func() error {
			ids, err := s.postStore.ListAuthorsIDsBefore(gctx, []uint64{t.AuthorID}, t.ColdBefore, want, s.cfg.AuthorInChunkSize)
			if err != nil {
				return err
			}
			extraLists[slot] = ids
			return nil
		})
	}
	if plan.NeedInbox && len(effectiveNormals) > 0 {
		slot := idx
		coldBefore := plan.InboxColdBefore
		g.Go(func() error {
			ids, err := s.postStore.ListAuthorsIDsBefore(gctx, effectiveNormals, coldBefore, want, s.cfg.AuthorInChunkSize)
			if err != nil {
				return err
			}
			extraLists[slot] = ids
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	allLists := make([][]uint64, 0, len(extraLists)+1)
	allLists = append(allLists, cand)
	allLists = append(allLists, extraLists...)
	return MergeDescUnique(allLists, want), refreshedFollowees, nil
}

// rebuildInboxAndSlice 在用户条纹锁内执行双重检查、重建收件箱并两段补漏。
// 始终在锁内解析最新关注列表，杜绝并发关注/取关交错导致把旧列表写回脏箱。
func (s *FollowingFeedService) rebuildInboxAndSlice(
	ctx context.Context,
	userID uint64,
	bigVSet map[uint64]struct{},
	want int,
) (TimelineBucket, []uint64, map[uint64]struct{}, error) {
	mu := s.userLock(userID)
	mu.Lock()
	defer mu.Unlock()

	// 双重检查：排队期间可能已由另一请求建好收件箱。
	if rr, err := s.feedRepo.RangeInbox(ctx, userID, 0, want); err == nil && rr.Exists {
		return TimelineBucket{
			AuthorID:  0,
			IDs:       rr.IDs,
			SafeFloor: ComputeSafeFloor(true, rr.Card, s.feedRepo.InboxHotSize(), rr.MinID),
		}, nil, nil, nil
	}

	// 锁内始终按最新关注列表重建，杜绝“等锁期间触发 Unfollow 且另一请求重填 L1”的三协程交错写回脏箱竞态。
	var latestIDs []uint64
	if raw, ok := s.followingIDsCache.GetLocal(userID); ok {
		parsed, parseErr := unmarshalUint64Slice(raw)
		if parseErr == nil {
			latestIDs = parsed
		}
	}
	if latestIDs == nil {
		loaded, err := s.loadFollowingIDsLocked(ctx, userID)
		if err != nil {
			return TimelineBucket{}, nil, nil, err
		}
		latestIDs = loaded
	}
	latestIDs, refreshedFollowees := normalizeFollowees(latestIDs, userID)
	currentNormals, _ := splitAuthorsByBigV(latestIDs, bigVSet)
	if len(currentNormals) == 0 {
		return TimelineBucket{}, currentNormals, refreshedFollowees, nil
	}

	hotSize := s.feedRepo.InboxHotSize()
	builtIDs, err := s.postStore.ListAuthorsIDsBefore(ctx, currentNormals, 0, hotSize, s.cfg.AuthorInChunkSize)
	if err != nil {
		return TimelineBucket{}, nil, nil, err
	}
	if err := s.feedRepo.ReplaceInbox(ctx, userID, builtIDs); err != nil {
		slog.WarnContext(ctx, "replace following inbox failed", "user_id", userID, "err", err)
	}

	// 两段补漏：把建箱查询瞬间普通作者新发的帖子补入收件箱。
	var maxBuilt uint64
	if len(builtIDs) > 0 {
		maxBuilt = builtIDs[0]
	}
	finalHot := builtIDs
	newer, err := s.postStore.ListAuthorsNewerIDs(ctx, currentNormals, maxBuilt, hotSize, s.cfg.AuthorInChunkSize)
	if err != nil {
		slog.WarnContext(ctx, "catch up newer posts for inbox failed", "user_id", userID, "err", err)
	} else if len(newer) > 0 {
		if _, addErr := s.feedRepo.AddToInboxIfExists(ctx, userID, newer...); addErr != nil {
			slog.WarnContext(ctx, "add catch-up posts to inbox failed", "user_id", userID, "err", addErr)
		}
		finalHot = MergeDescUnique([][]uint64{newer, builtIDs}, hotSize)
	}

	var safeFloor uint64
	if len(finalHot) >= hotSize {
		safeFloor = finalHot[hotSize-1]
	}
	return TimelineBucket{
		AuthorID:  0,
		IDs:       filterBeforeLimit(finalHot, 0, want),
		SafeFloor: safeFloor,
	}, currentNormals, refreshedFollowees, nil
}

// rebuildOutbox 通过 singleflight 按大 V 维度合并重建其发件箱热窗口并执行两段补漏。
func (s *FollowingFeedService) rebuildOutbox(ctx context.Context, authorID uint64) ([]uint64, error) {
	v, err, _ := s.outboxSF.Do(strconv.FormatUint(authorID, 10), func() (any, error) {
		// 使用独立短超时上下文，避免首个请求客户端断开连累共享同次重建的其他粉丝。
		sfCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sfRebuildTimeout)
		defer cancel()

		hotSize := s.feedRepo.OutboxHotSize()
		builtIDs, err := s.postStore.ListAuthorsIDsBefore(sfCtx, []uint64{authorID}, 0, hotSize, s.cfg.AuthorInChunkSize)
		if err != nil {
			return nil, err
		}
		if err := s.feedRepo.ReplaceOutbox(sfCtx, authorID, builtIDs); err != nil {
			slog.WarnContext(sfCtx, "replace bigv outbox failed", "author_id", authorID, "err", err)
		}

		var maxBuilt uint64
		if len(builtIDs) > 0 {
			maxBuilt = builtIDs[0]
		}
		finalHot := builtIDs
		newer, err := s.postStore.ListAuthorsNewerIDs(sfCtx, []uint64{authorID}, maxBuilt, hotSize, s.cfg.AuthorInChunkSize)
		if err != nil {
			slog.WarnContext(sfCtx, "catch up newer posts for outbox failed", "author_id", authorID, "err", err)
		} else if len(newer) > 0 {
			if _, addErr := s.feedRepo.AddToOutboxIfExists(sfCtx, authorID, newer...); addErr != nil {
				slog.WarnContext(sfCtx, "add catch-up posts to outbox failed", "author_id", authorID, "err", addErr)
			}
			finalHot = MergeDescUnique([][]uint64{newer, builtIDs}, hotSize)
		}
		return finalHot, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]uint64), nil
}

// getFollowingIDs 优先无锁命中 L1；未命中时持用户条纹锁执行 L2/L3 读取与回填，避免与 OnFollowChanged 并发写回旧列表。
func (s *FollowingFeedService) getFollowingIDs(ctx context.Context, userID uint64) ([]uint64, error) {
	if raw, ok := s.followingIDsCache.GetLocal(userID); ok {
		if ids, err := unmarshalUint64Slice(raw); err == nil {
			return ids, nil
		}
	}

	mu := s.userLock(userID)
	mu.Lock()
	defer mu.Unlock()
	return s.loadFollowingIDsLocked(ctx, userID)
}

func (s *FollowingFeedService) loadFollowingIDsLocked(ctx context.Context, userID uint64) ([]uint64, error) {
	load := func(ctx context.Context, missing []uint64) (map[uint64][]byte, error) {
		out := make(map[uint64][]byte, len(missing))
		for _, uid := range missing {
			ids, err := s.followStore.ListAllFolloweeIDs(ctx, uid)
			if err != nil {
				return nil, err
			}
			if ids == nil {
				ids = []uint64{}
			}
			b, err := json.Marshal(ids)
			if err != nil {
				return nil, fmt.Errorf("marshal following ids: %w", err)
			}
			out[uid] = b
		}
		return out, nil
	}
	redisTTL := time.Duration(s.cfg.FollowingIDsRedisTTL) * time.Second
	ttl := func(uint64) time.Duration { return redisTTL }

	vals, err := s.followingIDsCache.GetMany(ctx, []uint64{userID}, load, ttl)
	if err != nil {
		return nil, err
	}
	raw, ok := vals[userID]
	if !ok {
		return []uint64{}, nil
	}
	ids, err := unmarshalUint64Slice(raw)
	if err != nil {
		slog.WarnContext(ctx, "bad following_ids cache, fallback to mysql", "user_id", userID, "err", err)
		return s.followStore.ListAllFolloweeIDs(ctx, userID)
	}
	return ids, nil
}

// getBigVSet 按 L1 进程快照（30s）-> Redis feed:bigvs -> MySQL 的顺序加载全站大 V 集合。
func (s *FollowingFeedService) getBigVSet(ctx context.Context) (map[uint64]struct{}, error) {
	now := time.Now()
	if snap := s.bigVSnap.Load(); snap != nil && now.Before(snap.expiresAt) {
		return snap.ids, nil
	}

	v, err, _ := s.bigVSF.Do("bigvs", func() (any, error) {
		now := time.Now()
		if snap := s.bigVSnap.Load(); snap != nil && now.Before(snap.expiresAt) {
			return snap.ids, nil
		}
		sfCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bigVLoadTimeout)
		defer cancel()

		ttl := time.Duration(s.cfg.BigVLocalTTL) * time.Second
		ids, initialized, err := s.feedRepo.GetBigVs(sfCtx)
		if err == nil && initialized {
			s.bigVSnap.Store(&bigVSnapshot{ids: ids, expiresAt: time.Now().Add(ttl)})
			return ids, nil
		}
		if err != nil {
			slog.WarnContext(sfCtx, "read feed:bigvs from redis failed, fallback to mysql", "err", err)
		}

		list, dbErr := s.followStore.ListBigVIDs(sfCtx)
		if dbErr != nil {
			return nil, dbErr
		}
		if err == nil && !initialized {
			if repErr := s.feedRepo.ReplaceBigVs(sfCtx, list); repErr != nil {
				slog.WarnContext(sfCtx, "init feed:bigvs in redis failed", "err", repErr)
			}
		}
		m := make(map[uint64]struct{}, len(list))
		for _, id := range list {
			if id > 0 {
				m[id] = struct{}{}
			}
		}
		s.bigVSnap.Store(&bigVSnapshot{ids: m, expiresAt: time.Now().Add(ttl)})
		return m, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(map[uint64]struct{}), nil
}

func unmarshalUint64Slice(raw []byte) ([]uint64, error) {
	if len(raw) == 0 {
		return []uint64{}, nil
	}
	var ids []uint64
	if err := json.Unmarshal(raw, &ids); err != nil {
		return nil, err
	}
	if ids == nil {
		return []uint64{}, nil
	}
	return ids, nil
}

func normalizeFollowees(raw []uint64, selfID uint64) ([]uint64, map[uint64]struct{}) {
	if len(raw) == 0 {
		return nil, nil
	}
	set := make(map[uint64]struct{}, len(raw))
	out := make([]uint64, 0, len(raw))
	for _, id := range raw {
		if id == 0 || id == selfID {
			continue
		}
		if _, dup := set[id]; dup {
			continue
		}
		set[id] = struct{}{}
		out = append(out, id)
	}
	return out, set
}

func splitAuthorsByBigV(followees []uint64, bigVSet map[uint64]struct{}) (normalAuthors, bigVAuthors []uint64) {
	normalAuthors = make([]uint64, 0, len(followees))
	for _, id := range followees {
		if _, isBigV := bigVSet[id]; isBigV {
			bigVAuthors = append(bigVAuthors, id)
		} else {
			normalAuthors = append(normalAuthors, id)
		}
	}
	return normalAuthors, bigVAuthors
}
