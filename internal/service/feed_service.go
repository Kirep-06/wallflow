package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Kirep-06/gocache"

	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
)

// LatestRebuilder 负责最新流热窗口的重建与对账，API 与 Worker 共用。
type LatestRebuilder struct {
	store   PostStore
	feed    *repository.LatestFeedRepo
	running atomic.Bool
}

func NewLatestRebuilder(store PostStore, feed *repository.LatestFeedRepo) *LatestRebuilder {
	return &LatestRebuilder{store: store, feed: feed}
}

// Rebuild 从 MySQL 重建热窗口。跨进程由 Redis 锁保证只有一个执行者，未抢到锁直接返回。
func (r *LatestRebuilder) Rebuild(ctx context.Context) error {
	token, err := r.feed.TryLock(ctx)
	if err != nil {
		return fmt.Errorf("rebuild lock: %w", err)
	}
	if token == "" {
		return nil
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		if err := r.feed.Unlock(uctx, token); err != nil {
			slog.Warn("release rebuild lock failed", "err", err)
		}
	}()

	ids, err := r.store.ListIDsBefore(ctx, 0, r.feed.HotSize())
	if err != nil {
		return err
	}
	if err := r.feed.Replace(ctx, ids); err != nil {
		return err
	}
	// 补漏：重建期间新发的帖子因 ready 尚不存在被 Lua 跳过，这里补回。
	var maxBuilt uint64
	if len(ids) > 0 {
		maxBuilt = ids[0]
	}
	newer, err := r.store.ListNewerIDs(ctx, maxBuilt, r.feed.HotSize())
	if err != nil {
		return err
	}
	if _, err := r.feed.Add(ctx, newer...); err != nil {
		return err
	}
	slog.InfoContext(ctx, "latest feed rebuilt", "size", len(ids), "catch_up", len(newer))
	return nil
}

// TriggerAsync 在后台异步重建；同一进程内同时最多一个重建协程。
func (r *LatestRebuilder) TriggerAsync() {
	if !r.running.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer r.running.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := r.Rebuild(ctx); err != nil {
			slog.Error("rebuild latest feed failed", "err", err)
		}
	}()
}

// Reconcile 对账：热窗口未就绪则重建；否则把 MySQL 最新 hotSize 条幂等补入 ZSet，修复发帖时 Redis 写失败造成的缺口。
func (r *LatestRebuilder) Reconcile(ctx context.Context) error {
	ready, err := r.feed.IsReady(ctx)
	if err != nil {
		return fmt.Errorf("check ready: %w", err)
	}
	if !ready {
		return r.Rebuild(ctx)
	}
	ids, err := r.store.ListIDsBefore(ctx, 0, r.feed.HotSize())
	if err != nil {
		return err
	}
	_, err = r.feed.Add(ctx, ids...)
	return err
}

// FeedService 编排最新流：Redis 热窗口 + MySQL 冷数据 + 三级缓存详情。
type FeedService struct {
	store     PostStore
	feed      *repository.LatestFeedRepo
	posts     *PostService
	rebuilder *LatestRebuilder
	local     *gocache.Cache
	cfg       config.FeedConfig
	firstTTL  int // 首页 ID 列表的 L1 TTL（秒）
}

func NewFeedService(store PostStore, feed *repository.LatestFeedRepo, posts *PostService, rebuilder *LatestRebuilder,
	local *gocache.Cache, cfg config.FeedConfig, firstPageTTL int) *FeedService {
	return &FeedService{store: store, feed: feed, posts: posts, rebuilder: rebuilder, local: local, cfg: cfg, firstTTL: firstPageTTL}
}

// Latest 返回最新流的一页。
func (s *FeedService) Latest(ctx context.Context, cursorStr string, limit int) (*model.FeedPage, error) {
	before, err := cursor.DecodeID(cursorStr)
	if err != nil {
		return nil, errno.ErrBadCursor
	}
	if limit <= 0 {
		limit = s.cfg.PageDefault
	}
	limit = min(limit, s.cfg.PageMax)

	ids, err := s.pageIDs(ctx, before, limit)
	if err != nil {
		return nil, err
	}
	page := &model.FeedPage{HasMore: len(ids) > limit}
	if page.HasMore {
		ids = ids[:limit]
	}
	if page.HasMore && len(ids) > 0 {
		page.NextCursor = cursor.EncodeID(ids[len(ids)-1])
	}
	if page.Items, err = s.posts.GetPosts(ctx, ids); err != nil {
		return nil, err
	}
	return page, nil
}

// pageIDs 返回最多 limit+1 个 ID（多取 1 个用于判断 has_more）；首页默认条数走 L1 短 TTL 缓存。
func (s *FeedService) pageIDs(ctx context.Context, before uint64, limit int) ([]uint64, error) {
	useL1 := before == 0 && limit == s.cfg.PageDefault && s.firstTTL > 0
	l1Key := []byte(fmt.Sprintf("feed:latest:first:%d", limit))
	if useL1 {
		if b, err := s.local.Get(l1Key); err == nil {
			var ids []uint64
			if json.Unmarshal(b, &ids) == nil {
				return ids, nil
			}
		}
	}
	ids, err := s.latestIDs(ctx, before, limit+1)
	if err != nil {
		return nil, err
	}
	if useL1 {
		if b, err := json.Marshal(ids); err == nil {
			_ = s.local.Set(l1Key, b, s.firstTTL)
		}
	}
	return ids, nil
}

// latestIDs 冷热衔接：优先读 Redis 热窗口，翻过热窗口尾部后回源 MySQL。
func (s *FeedService) latestIDs(ctx context.Context, before uint64, want int) ([]uint64, error) {
	rr, err := s.feed.Range(ctx, before, want)
	if err != nil {
		slog.WarnContext(ctx, "latest feed redis unavailable, fallback to mysql", "err", err)
		return s.store.ListIDsBefore(ctx, before, want)
	}
	if !rr.Ready {
		s.rebuilder.TriggerAsync()
		return s.store.ListIDsBefore(ctx, before, want)
	}
	ids := rr.IDs
	// 热窗口已满且本页没取够，说明已翻到热窗口尾部，剩余部分属于冷数据。
	// 热窗口未满则全站数据都在 Redis 中，无需回源。
	if len(ids) < want && rr.Card >= int64(s.feed.HotSize()) {
		last := before
		if len(ids) > 0 {
			last = ids[len(ids)-1]
		}
		cold, err := s.store.ListIDsBefore(ctx, last, want-len(ids))
		if err != nil {
			return nil, err
		}
		ids = append(ids, cold...)
	}
	return ids, nil
}
