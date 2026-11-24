package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"wallpaper-feed/internal/app"
	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/jwtutil"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/service"
	"wallpaper-feed/internal/storage"
	"wallpaper-feed/internal/worker"
)

const (
	benchJWTSecret = "wallflow_bench_jwt_secret_at_least_32_bytes_long"
	benchJWTIssuer = "wallpaper-feed"
)

// benchSuite 封装完整的压测运行环境（复用真实 app.App 路由、缓存、仓储与 Worker）。
type benchSuite struct {
	cfg     *config.Config
	app     *app.App
	meta    *SeedMeta
	server  *httptest.Server
	client  *http.Client
	jwtMgr  *jwtutil.Manager
	sessRep *repository.AuthSessionRepository

	wpRepo          *repository.WallpaperRepository
	userRepo        *repository.UserRepository
	interactionRepo *repository.InteractionRepository
	followRepo      *repository.FollowRepository
	store           storage.StorageService
	l2OnlyWpSvc     *service.WallpaperService
	hotRank         *cache.HotRank
	bigVSet         *cache.BigVSet
	readerToken     string
	fanTokens       []string
}

func newBenchSuite(ctx context.Context, cfg *config.Config, meta *SeedMeta) (*benchSuite, error) {
	// 压测期间将全局日志调为 Error，避免 stdout I/O 干扰压测延迟
	gin.SetMode(gin.ReleaseMode)
	a, err := app.New(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("init app: %w", err)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))

	jwtMgr, err := jwtutil.NewManager(cfg.JWTSecret, benchJWTIssuer, 2*time.Hour)
	if err != nil {
		_ = a.Close()
		return nil, err
	}
	store, err := storage.New(cfg.Storage)
	if err != nil {
		_ = a.Close()
		return nil, err
	}

	wpRepo := repository.NewWallpaperRepository(a.DB)
	userRepo := repository.NewUserRepository(a.DB)
	metaRepo := repository.NewMetaRepository(a.DB)
	followRepo := repository.NewFollowRepository(a.DB)
	interactionRepo := repository.NewInteractionRepository(a.DB)
	sessRep := repository.NewAuthSessionRepository(a.Redis)

	// 构造仅开启 L2 Redis（l1=nil）的对照 WallpaperService，用于隔离衡量 L1 gocache 收益
	l2ProfileCache := cache.NewMultiLevel[model.UserProfileCache](nil, a.Redis, 30*time.Second, 30*time.Minute)
	l2WpCache := cache.NewMultiLevel(nil, a.Redis, 30*time.Second, 10*time.Minute, cache.WithL2TTLFunc(service.WallpaperDetailL2TTL))
	l2StatsCache := cache.NewMultiLevel[model.WallpaperStatsCache](nil, a.Redis, 10*time.Second, 10*time.Minute)
	l2FollowStats := cache.NewMultiLevel[model.FollowStatsCache](nil, a.Redis, 15*time.Second, 10*time.Minute)
	l2FollowingIDs := cache.NewMultiLevel[[]uint64](nil, a.Redis, 15*time.Second, 10*time.Minute)
	l2UserSvc := service.NewUserService(service.UserServiceDeps{
		UserRepo:          userRepo,
		WallpaperRepo:     wpRepo,
		FollowRepo:        followRepo,
		ProfileCache:      l2ProfileCache,
		FollowStatsCache:  l2FollowStats,
		FollowingIDsCache: l2FollowingIDs,
		Store:             store,
	})
	l2WpSvc := service.NewWallpaperService(
		metaRepo, wpRepo, l2UserSvc, l2WpCache, cache.NewLatestFeed(a.Redis), store,
		service.WithWallpaperInteraction(interactionRepo, l2StatsCache),
	)

	s := &benchSuite{
		cfg:             cfg,
		app:             a,
		meta:            meta,
		jwtMgr:          jwtMgr,
		sessRep:         sessRep,
		wpRepo:          wpRepo,
		userRepo:        userRepo,
		interactionRepo: interactionRepo,
		followRepo:      followRepo,
		store:           store,
		l2OnlyWpSvc:     l2WpSvc,
		hotRank:         cache.NewHotRank(a.Redis),
		bigVSet:         cache.NewBigVSet(a.Redis, cache.BigVL1TTL),
	}

	s.mountBaselineRoutes()
	s.server = httptest.NewServer(a.Router)
	s.client = &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     60 * time.Second,
		},
	}

	// 为 bench_reader 签发真实 JWT + Redis 会话
	s.readerToken, err = s.issueUserToken(ctx, meta.ReaderID)
	if err != nil {
		s.close()
		return nil, fmt.Errorf("issue reader token: %w", err)
	}
	return s, nil
}

func (s *benchSuite) close() {
	if s.server != nil {
		s.server.Close()
	}
	if s.client != nil {
		s.client.CloseIdleConnections()
	}
	if s.app != nil {
		_ = s.app.Close()
	}
}

func (s *benchSuite) issueUserToken(ctx context.Context, userID uint64) (string, error) {
	sid, err := jwtutil.GenerateSessionID()
	if err != nil {
		return "", err
	}
	if err := s.sessRep.Create(ctx, repository.AuthSession{
		SessionID:     sid,
		UserID:        userID,
		RefreshSHA256: "bench_refresh_placeholder",
		ExpiresAt:     time.Now().UTC().Add(24 * time.Hour),
	}); err != nil {
		return "", err
	}
	tok, _, err := s.jwtMgr.GenerateAccessToken(userID, sid)
	if err != nil {
		return "", err
	}
	return "Bearer " + tok, nil
}

// mountBaselineRoutes 仅在压测进程内挂载对照组路由（不改动生产 internal/handler/router.go）。
func (s *benchSuite) mountBaselineRoutes() {
	g := s.app.Router.Group("/bench/v1")

	// 对照 A1-1：壁纸详情直查 MySQL（绕过 L1/L2）
	g.GET("/wallpapers/:id/direct-mysql", func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil || id == 0 {
			c.Status(http.StatusBadRequest)
			return
		}
		detail, err := s.directMySQLDetail(c.Request.Context(), id)
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		response.OK(c, detail)
	})

	// 对照 A1-2：壁纸详情仅走 L2 Redis（关闭 L1 gocache）
	g.GET("/wallpapers/:id/l2-only", func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil || id == 0 {
			c.Status(http.StatusBadRequest)
			return
		}
		detail, err := s.l2OnlyWpSvc.GetWallpaperDetail(c.Request.Context(), id, 0)
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		response.OK(c, detail)
	})

	// 对照 B1-1：下载计数同步直写 wallpaper_stats（无 MQ 削峰）
	g.POST("/wallpapers/:id/download-sync", func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil || id == 0 {
			c.Status(http.StatusBadRequest)
			return
		}
		delta := []model.StatsDelta{{WallpaperID: id, Download: 1}}
		if err := s.interactionRepo.BatchApplyStatsDeltas(c.Request.Context(), delta); err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		response.OK(c, gin.H{"ok": true})
	})

	// 对照 B2-1：点赞同步事务直写（wallpaper_likes + wallpaper_stats 行锁争用）
	g.POST("/wallpapers/:id/like-sync", func(c *gin.Context) {
		wpID, err := strconv.ParseUint(c.Param("id"), 10, 64)
		uid, err2 := strconv.ParseUint(c.GetHeader("X-Bench-UID"), 10, 64)
		if err != nil || err2 != nil || wpID == 0 || uid == 0 {
			c.Status(http.StatusBadRequest)
			return
		}
		err = s.app.DB.WithContext(c.Request.Context()).Transaction(func(tx *gorm.DB) error {
			res := tx.Exec(
				"INSERT IGNORE INTO wallpaper_likes (user_id, wallpaper_id, created_at) VALUES (?, ?, ?)",
				uid, wpID, time.Now().UTC(),
			)
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected > 0 {
				return tx.Exec(
					"INSERT INTO wallpaper_stats (wallpaper_id, like_count, fav_count, download_count, comment_count) VALUES (?, 1, 0, 0, 0) ON DUPLICATE KEY UPDATE like_count = like_count + 1",
					wpID,
				).Error
			}
			return nil
		})
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		response.OK(c, model.LikeActionResult{IsLiked: true})
	})

	// 对照 C1：关注流纯 SQL 拉取（IN + ORDER BY + 直查 MySQL 装配）
	g.GET("/feed/following-sql", func(c *gin.Context) {
		ctx := c.Request.Context()
		followees, err := s.followRepo.ListAllFollowingIDs(ctx, s.meta.ReaderID)
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		ids, err := s.wpRepo.ListAuthorsWallpaperIDs(ctx, followees, 0, 21)
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		hasMore := len(ids) > 20
		if hasMore {
			ids = ids[:20]
		}
		items, err := s.directMySQLSummaries(ctx, ids)
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		response.OK(c, response.NewPageData(items, "", hasMore))
	})

	// 对照 C2：关注流纯 SQL 拉取 ID（IN + ORDER BY，不装配卡片，单独隔离 SQL 瓶颈）
	g.GET("/feed/following-sql-ids", func(c *gin.Context) {
		ctx := c.Request.Context()
		ids, err := s.wpRepo.ListAuthorsWallpaperIDs(ctx, s.meta.FollowedIDs, 0, 21)
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		response.OK(c, gin.H{"ids": ids})
	})
}

func (s *benchSuite) directMySQLDetail(ctx context.Context, id uint64) (*model.WallpaperDetail, error) {
	found, err := s.wpRepo.FindDetailsByIDs(ctx, []uint64{id})
	if err != nil {
		return nil, err
	}
	snap, ok := found[id]
	if !ok {
		return nil, fmt.Errorf("wallpaper %d not found", id)
	}
	authorRow, err := s.userRepo.FindProfileByID(ctx, snap.UserID)
	if err != nil {
		return nil, err
	}
	stats, err := s.interactionRepo.FindStatsByWallpaperID(ctx, id)
	if err != nil {
		return nil, err
	}
	imgs := make([]model.WallpaperImageItem, 0, len(snap.Images))
	for _, im := range snap.Images {
		imgs = append(imgs, model.WallpaperImageItem{
			ID:          im.ID,
			SortNo:      im.SortNo,
			ThumbURL:    s.store.BuildThumbURL(im.ObjectKey),
			RawURL:      s.store.BuildRawURL(im.ObjectKey),
			Width:       im.Width,
			Height:      im.Height,
			Orientation: im.Orientation,
			Is4K:        im.Is4K,
			SizeBytes:   im.SizeBytes,
		})
	}
	return &model.WallpaperDetail{
		ID:          snap.ID,
		Title:       snap.Title,
		Description: snap.Description,
		ThumbURL:    s.store.BuildThumbURL(snap.ObjectKey),
		RawURL:      s.store.BuildRawURL(snap.ObjectKey),
		Width:       snap.Width,
		Height:      snap.Height,
		Orientation: snap.Orientation,
		Is4K:        snap.Is4K,
		SizeBytes:   snap.SizeBytes,
		Images:      imgs,
		Category:    snap.Category,
		Tags:        snap.Tags,
		Author: model.WallpaperAuthor{
			ID:        authorRow.ID,
			Username:  authorRow.Username,
			Nickname:  authorRow.Nickname,
			AvatarURL: s.store.BuildRawURL(authorRow.AvatarKey),
		},
		LikeCount:     stats.LikeCount,
		FavCount:      stats.FavCount,
		DownloadCount: stats.DownloadCount,
		CommentCount:  stats.CommentCount,
		CreatedAt:     snap.CreatedAt,
	}, nil
}

func (s *benchSuite) directMySQLSummaries(ctx context.Context, ids []uint64) ([]model.WallpaperSummary, error) {
	details, err := s.wpRepo.FindDetailsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	authorSet := make(map[uint64]struct{}, len(details))
	authorIDs := make([]uint64, 0, len(details))
	for _, d := range details {
		if _, ok := authorSet[d.UserID]; !ok {
			authorSet[d.UserID] = struct{}{}
			authorIDs = append(authorIDs, d.UserID)
		}
	}
	authors, err := s.userRepo.FindProfilesByIDs(ctx, authorIDs)
	if err != nil {
		return nil, err
	}
	statsMap, err := s.interactionRepo.FindStatsByWallpaperIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	likedSet, err := s.interactionRepo.BatchCheckRelations(ctx, model.InteractionKindLike, s.meta.ReaderID, ids)
	if err != nil {
		return nil, err
	}
	favSet, err := s.interactionRepo.BatchCheckRelations(ctx, model.InteractionKindFavorite, s.meta.ReaderID, ids)
	if err != nil {
		return nil, err
	}

	out := make([]model.WallpaperSummary, 0, len(ids))
	for _, id := range ids {
		d, ok := details[id]
		if !ok {
			continue
		}
		au := authors[d.UserID]
		st := statsMap[id]
		_, isLiked := likedSet[id]
		_, isFav := favSet[id]
		out = append(out, model.WallpaperSummary{
			ID:            d.ID,
			Title:         d.Title,
			ThumbURL:      s.store.BuildThumbURL(d.ObjectKey),
			Width:         d.Width,
			Height:        d.Height,
			Orientation:   d.Orientation,
			Is4K:          d.Is4K,
			Category:      d.Category,
			Author:        model.WallpaperAuthor{ID: au.ID, Username: au.Username, Nickname: au.Nickname, AvatarURL: s.store.BuildRawURL(au.AvatarKey)},
			LikeCount:     st.LikeCount,
			FavCount:      st.FavCount,
			DownloadCount: st.DownloadCount,
			CommentCount:  st.CommentCount,
			IsLiked:       isLiked,
			IsFavorited:   isFav,
			CreatedAt:     d.CreatedAt,
		})
	}
	return out, nil
}

// runGroupARead 执行 (a) 读链路对照压测：直查 MySQL vs L2 Redis vs 三级缓存、singleflight 击穿、热榜 200 vs ETag 304。
func (s *benchSuite) runGroupARead(ctx context.Context) ([]BenchResult, error) {
	fmt.Println("=== [Group A] Read Path Benchmarks ===")
	var results []BenchResult
	hotIDs := s.meta.WallpaperIDs // 最新 200 条热点壁纸

	// A1-1: 直查 MySQL
	rDirect := runLoad(ctx, "A1-读链路", "详情直查 MySQL (无缓存)", 50, 3000, func(ctx context.Context, i int) (int64, error) {
		id := hotIDs[i%len(hotIDs)]
		return doHTTPRequest(ctx, s.client, http.MethodGet,
			fmt.Sprintf("%s/bench/v1/wallpapers/%d/direct-mysql", s.server.URL, id), nil, http.StatusOK)
	})
	rDirect.Extra = fmt.Sprintf("avg=%dB", rDirect.AvgBytes)
	results = append(results, rDirect)
	fmt.Printf("  [A1-1] Direct MySQL: QPS=%.1f P50=%s P95=%s P99=%s\n",
		rDirect.QPS, formatDurationMS(rDirect.P50Latency), formatDurationMS(rDirect.P95Latency), formatDurationMS(rDirect.P99Latency))

	// 预热 200 条热点壁纸进入 L2 Redis 与 L1 gocache
	for _, id := range hotIDs {
		_, _ = doHTTPRequest(ctx, s.client, http.MethodGet,
			fmt.Sprintf("%s/api/v1/wallpapers/%d", s.server.URL, id), nil, http.StatusOK)
	}

	// A1-2: 仅 L2 Redis 命中（关闭 L1）
	rL2 := runLoad(ctx, "A1-读链路", "详情仅 L2 Redis (无 L1)", 50, 5000, func(ctx context.Context, i int) (int64, error) {
		id := hotIDs[i%len(hotIDs)]
		return doHTTPRequest(ctx, s.client, http.MethodGet,
			fmt.Sprintf("%s/bench/v1/wallpapers/%d/l2-only", s.server.URL, id), nil, http.StatusOK)
	})
	rL2.Extra = fmt.Sprintf("vs MySQL %.1fx", rL2.QPS/max(rDirect.QPS, 1))
	results = append(results, rL2)
	fmt.Printf("  [A1-2] L2 Redis Only: QPS=%.1f P50=%s P95=%s P99=%s (%.1fx)\n",
		rL2.QPS, formatDurationMS(rL2.P50Latency), formatDurationMS(rL2.P95Latency), formatDurationMS(rL2.P99Latency), rL2.QPS/max(rDirect.QPS, 1))

	// A1-3: 生产三级缓存（L1 gocache + L2 Redis + singleflight）
	rL1 := runLoad(ctx, "A1-读链路", "详情三级缓存 (L1 gocache+L2)", 50, 20000, func(ctx context.Context, i int) (int64, error) {
		id := hotIDs[i%len(hotIDs)]
		return doHTTPRequest(ctx, s.client, http.MethodGet,
			fmt.Sprintf("%s/api/v1/wallpapers/%d", s.server.URL, id), nil, http.StatusOK)
	})
	rL1.Extra = fmt.Sprintf("vs MySQL %.1fx", rL1.QPS/max(rDirect.QPS, 1))
	results = append(results, rL1)
	fmt.Printf("  [A1-3] 3-Level Cache (L1+L2): QPS=%.1f P50=%s P95=%s P99=%s (%.1fx)\n",
		rL1.QPS, formatDurationMS(rL1.P50Latency), formatDurationMS(rL1.P95Latency), formatDurationMS(rL1.P99Latency), rL1.QPS/max(rDirect.QPS, 1))

	// A1-4: 单热 Key 缓存击穿 singleflight 合并验证（C=100 瞬时并发冷 Key）
	var dbCallsDirect, dbCallsSF atomic.Int64
	coldKey := fmt.Sprintf("bench:sf:wp:%d", s.meta.ColdWallpaper)
	_ = s.app.Redis.Del(ctx, coldKey).Err()
	sfCache := cache.NewMultiLevel[model.WallpaperDetailCache](nil, s.app.Redis, 30*time.Second, 10*time.Minute)

	runStampede := func(useSF bool) time.Duration {
		var wg sync.WaitGroup
		start := make(chan struct{})
		t0 := time.Now()
		for range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if !useSF {
					dbCallsDirect.Add(1)
					_, _ = s.wpRepo.FindDetailsByIDs(ctx, []uint64{s.meta.ColdWallpaper})
				} else {
					_, _ = sfCache.Get(ctx, coldKey, func(loadCtx context.Context) (model.WallpaperDetailCache, error) {
						dbCallsSF.Add(1)
						m, err := s.wpRepo.FindDetailsByIDs(loadCtx, []uint64{s.meta.ColdWallpaper})
						if err != nil {
							return model.WallpaperDetailCache{}, err
						}
						return m[s.meta.ColdWallpaper], nil
					})
				}
			}()
		}
		close(start)
		wg.Wait()
		return time.Since(t0)
	}
	dDirectStampede := runStampede(false)
	dSFStampede := runStampede(true)
	fmt.Printf("  [A1-4] Cold Hot-Key Stampede (C=100): Direct=%d DB queries (%s) vs singleflight=%d DB query (%s)\n",
		dbCallsDirect.Load(), formatDurationMS(dDirectStampede), dbCallsSF.Load(), formatDurationMS(dSFStampede))

	// A2: 热榜快照预热 + 200 全量 vs 弱 ETag 304 短路
	tHot0 := time.Now()
	hotEntries, err := model.ComputeHotTopN(ctx, s.wpRepo.ListHotRankBatch, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("compute hot topN: %w", err)
	}
	ver, err := s.hotRank.Swap(ctx, hotEntries)
	if err != nil {
		return nil, fmt.Errorf("swap hot rank: %w", err)
	}
	fmt.Printf("  [A2-warmup] ComputeHotTopN over %d wallpapers: count=%d ver=%s cost=%s\n",
		s.meta.TotalWallpapers, len(hotEntries), ver, formatDurationMS(time.Since(tHot0)))

	// 探测首屏 ETag 并预热 Top 20 壁纸详情/作者/统计缓存
	hotURL := fmt.Sprintf("%s/api/v1/feed/hot?limit=20", s.server.URL)
	probeResp, err := s.client.Get(hotURL)
	if err != nil {
		return nil, fmt.Errorf("probe hot feed: %w", err)
	}
	_, _ = io.Copy(io.Discard, probeResp.Body)
	_ = probeResp.Body.Close()
	etag := probeResp.Header.Get("ETag")
	if probeResp.StatusCode != http.StatusOK || etag == "" {
		return nil, fmt.Errorf("probe hot feed failed: status=%d etag=%q", probeResp.StatusCode, etag)
	}

	// A2-1: 热榜 200 全量返回
	rHot200 := runLoad(ctx, "A2-热榜协商", "热榜 GET /feed/hot (200 全量)", 50, 10000, func(ctx context.Context, _ int) (int64, error) {
		return doHTTPRequest(ctx, s.client, http.MethodGet, hotURL, nil, http.StatusOK)
	})
	rHot200.Extra = fmt.Sprintf("avg=%dB (%.1fMB)", rHot200.AvgBytes, float64(rHot200.TotalBytes)/(1024*1024))
	results = append(results, rHot200)
	fmt.Printf("  [A2-1] Hot Feed 200 Full: QPS=%.1f P50=%s P95=%s P99=%s avg=%dB\n",
		rHot200.QPS, formatDurationMS(rHot200.P50Latency), formatDurationMS(rHot200.P95Latency), formatDurationMS(rHot200.P99Latency), rHot200.AvgBytes)

	// A2-2: 热榜弱 ETag 304 短路
	etagHeaders := map[string]string{"If-None-Match": etag}
	rHot304 := runLoad(ctx, "A2-热榜协商", "热榜 GET /feed/hot (ETag 304 短路)", 50, 10000, func(ctx context.Context, _ int) (int64, error) {
		return doHTTPRequest(ctx, s.client, http.MethodGet, hotURL, etagHeaders, http.StatusNotModified)
	})
	rHot304.Extra = fmt.Sprintf("avg=%dB, vs 200 %.1fx", rHot304.AvgBytes, rHot304.QPS/max(rHot200.QPS, 1))
	results = append(results, rHot304)
	fmt.Printf("  [A2-2] Hot Feed 304 ETag: QPS=%.1f P50=%s P95=%s P99=%s avg=%dB (%.1fx)\n",
		rHot304.QPS, formatDurationMS(rHot304.P50Latency), formatDurationMS(rHot304.P95Latency), formatDurationMS(rHot304.P99Latency),
		rHot304.AvgBytes, rHot304.QPS/max(rHot200.QPS, 1))

	return results, nil
}

// runGroupBWrite 执行 (b) 互动写削峰对照压测：热点单帖并发下载/点赞，同步直写 vs PublishLight + Worker 批量刷盘。
func (s *benchSuite) runGroupBWrite(ctx context.Context) ([]BenchResult, error) {
	fmt.Println("=== [Group B] Interaction Write Peak-Shaving Benchmarks ===")
	var results []BenchResult

	// 选定 2 张独立热点壁纸分别测下载与点赞，并重置其初始计数为 0 以便精确对账
	dlWpID := s.meta.HotWallpaperID
	likeWpID := s.meta.WallpaperIDs[1]

	// 确保热点壁纸详情已在缓存中
	_, _ = doHTTPRequest(ctx, s.client, http.MethodGet,
		fmt.Sprintf("%s/api/v1/wallpapers/%d", s.server.URL, dlWpID), nil, http.StatusOK)
	_, _ = doHTTPRequest(ctx, s.client, http.MethodGet,
		fmt.Sprintf("%s/api/v1/wallpapers/%d", s.server.URL, likeWpID), nil, http.StatusOK)

	// 启动真实的 InteractionAggConsumer 消费压测专用的 RabbitMQ vhost
	workerCtx, workerCancel := context.WithCancel(ctx)
	aggConsumer := worker.NewInteractionAggConsumer(s.interactionRepo, s.app.Redis)
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- aggConsumer.Run(workerCtx, s.app.MQ)
	}()
	// 稍等消费者完成拓扑绑定并就绪
	time.Sleep(500 * time.Millisecond)

	defer func() {
		workerCancel()
		<-workerDone
	}()

	// --- B1: 热点单帖并发下载（C=50, N=3,000） ---
	const dlReqs = 3000
	if err := s.app.DB.WithContext(ctx).Exec(
		"UPDATE wallpaper_stats SET download_count = 0 WHERE wallpaper_id = ?", dlWpID,
	).Error; err != nil {
		return nil, err
	}

	// B1-1: 同步直写 wallpaper_stats
	dlSyncURL := fmt.Sprintf("%s/bench/v1/wallpapers/%d/download-sync", s.server.URL, dlWpID)
	rDLSync := runLoad(ctx, "B1-下载削峰", "热点单帖下载-同步直写 wallpaper_stats", 50, dlReqs, func(ctx context.Context, _ int) (int64, error) {
		return doHTTPRequest(ctx, s.client, http.MethodPost, dlSyncURL, nil, http.StatusOK)
	})
	rDLSync.Extra = fmt.Sprintf("%d次单行写锁", dlReqs)
	results = append(results, rDLSync)
	fmt.Printf("  [B1-1] Download Sync DB Write: QPS=%.1f P50=%s P95=%s P99=%s\n",
		rDLSync.QPS, formatDurationMS(rDLSync.P50Latency), formatDurationMS(rDLSync.P95Latency), formatDurationMS(rDLSync.P99Latency))

	// 重置下载计数为 0，测 B1-2 异步削峰
	if err := s.app.DB.WithContext(ctx).Exec(
		"UPDATE wallpaper_stats SET download_count = 0 WHERE wallpaper_id = ?", dlWpID,
	).Error; err != nil {
		return nil, err
	}
	dlAsyncURL := fmt.Sprintf("%s/api/v1/wallpapers/%d/download", s.server.URL, dlWpID)
	tAsyncStart := time.Now()
	rDLAsync := runLoad(ctx, "B1-下载削峰", "热点单帖下载-PublishLight+Worker批刷", 50, dlReqs, func(ctx context.Context, _ int) (int64, error) {
		return doHTTPRequest(ctx, s.client, http.MethodPost, dlAsyncURL, nil, http.StatusOK)
	})

	// 等待 Worker 排空并验证最终计数精确收敛 == dlReqs
	dlConverged, dlFinal, err := s.waitStatsCount(ctx, dlWpID, "download_count", uint64(dlReqs), 10*time.Second)
	if err != nil {
		return nil, err
	}
	dlConvergeCost := time.Since(tAsyncStart)
	rDLAsync.Extra = fmt.Sprintf("vs同步 %.1fx, 最终=%d/%d (%.2fs收敛, ~%d次批刷)",
		rDLAsync.QPS/max(rDLSync.QPS, 1), dlFinal, dlReqs, dlConvergeCost.Seconds(), (dlReqs+199)/200)
	results = append(results, rDLAsync)
	fmt.Printf("  [B1-2] Download PublishLight+Worker: QPS=%.1f P50=%s P95=%s P99=%s (%.1fx, converged=%v count=%d/%d in %s)\n",
		rDLAsync.QPS, formatDurationMS(rDLAsync.P50Latency), formatDurationMS(rDLAsync.P95Latency), formatDurationMS(rDLAsync.P99Latency),
		rDLAsync.QPS/max(rDLSync.QPS, 1), dlConverged, dlFinal, dlReqs, formatDurationMS(dlConvergeCost))

	// --- B2: 热点单帖并发点赞（1,000 个独立用户，C=50） ---
	const likeReqs = 1000
	if len(s.meta.FanUserIDs) < likeReqs {
		return nil, fmt.Errorf("need at least %d fan users for like bench, got %d", likeReqs, len(s.meta.FanUserIDs))
	}
	if len(s.fanTokens) < likeReqs {
		s.fanTokens = make([]string, likeReqs)
		for i := range likeReqs {
			tok, err := s.issueUserToken(ctx, s.meta.FanUserIDs[i])
			if err != nil {
				return nil, fmt.Errorf("issue fan token %d: %w", i, err)
			}
			s.fanTokens[i] = tok
		}
	}

	// B2-1: 同步事务直写（wallpaper_likes + wallpaper_stats 行锁争用）
	_ = s.app.DB.WithContext(ctx).Exec("DELETE FROM wallpaper_likes WHERE wallpaper_id = ?", likeWpID).Error
	_ = s.app.DB.WithContext(ctx).Exec("UPDATE wallpaper_stats SET like_count = 0 WHERE wallpaper_id = ?", likeWpID).Error
	likeSyncURL := fmt.Sprintf("%s/bench/v1/wallpapers/%d/like-sync", s.server.URL, likeWpID)
	rLikeSync := runLoad(ctx, "B2-点赞削峰", "热点单帖点赞-同步事务(明细+计数行锁)", 50, likeReqs, func(ctx context.Context, i int) (int64, error) {
		hdr := map[string]string{"X-Bench-UID": strconv.FormatUint(s.meta.FanUserIDs[i], 10)}
		return doHTTPRequest(ctx, s.client, http.MethodPost, likeSyncURL, hdr, http.StatusOK)
	})
	rLikeSync.Extra = fmt.Sprintf("%d次单行写锁", likeReqs)
	results = append(results, rLikeSync)
	fmt.Printf("  [B2-1] Like Sync TX: QPS=%.1f P50=%s P95=%s P99=%s\n",
		rLikeSync.QPS, formatDurationMS(rLikeSync.P50Latency), formatDurationMS(rLikeSync.P95Latency), formatDurationMS(rLikeSync.P99Latency))

	// B2-2: 生产异步削峰（POST /api/v1/wallpapers/:id/like -> 写明细 + PublishLight + Worker 批刷）
	_ = s.app.DB.WithContext(ctx).Exec("DELETE FROM wallpaper_likes WHERE wallpaper_id = ?", likeWpID).Error
	_ = s.app.DB.WithContext(ctx).Exec("UPDATE wallpaper_stats SET like_count = 0 WHERE wallpaper_id = ?", likeWpID).Error
	likeAsyncURL := fmt.Sprintf("%s/api/v1/wallpapers/%d/like", s.server.URL, likeWpID)
	tLikeStart := time.Now()
	rLikeAsync := runLoad(ctx, "B2-点赞削峰", "热点单帖点赞-明细+PublishLight+批刷", 50, likeReqs, func(ctx context.Context, i int) (int64, error) {
		hdr := map[string]string{"Authorization": s.fanTokens[i]}
		return doHTTPRequest(ctx, s.client, http.MethodPost, likeAsyncURL, hdr, http.StatusOK)
	})
	wantLikes := uint64(rLikeAsync.SuccessReqs)
	likeConverged, likeFinal, err := s.waitStatsCount(ctx, likeWpID, "like_count", wantLikes, 10*time.Second)
	if err != nil {
		return nil, err
	}
	likeConvergeCost := time.Since(tLikeStart)
	rLikeAsync.Extra = fmt.Sprintf("vs同步 %.1fx, 最终=%d/%d (%.2fs收敛, ~%d次批刷)",
		rLikeAsync.QPS/max(rLikeSync.QPS, 1), likeFinal, wantLikes, likeConvergeCost.Seconds(), (int(wantLikes)+199)/200)
	results = append(results, rLikeAsync)
	fmt.Printf("  [B2-2] Like Async+Worker: QPS=%.1f P50=%s P95=%s P99=%s (%.1fx, converged=%v count=%d/%d in %s)\n",
		rLikeAsync.QPS, formatDurationMS(rLikeAsync.P50Latency), formatDurationMS(rLikeAsync.P95Latency), formatDurationMS(rLikeAsync.P99Latency),
		rLikeAsync.QPS/max(rLikeSync.QPS, 1), likeConverged, likeFinal, wantLikes, formatDurationMS(likeConvergeCost))

	return results, nil
}

func (s *benchSuite) waitStatsCount(ctx context.Context, wpID uint64, col string, want uint64, timeout time.Duration) (bool, uint64, error) {
	deadline := time.Now().Add(timeout)
	var cur uint64
	for time.Now().Before(deadline) {
		err := s.app.DB.WithContext(ctx).
			Table("wallpaper_stats").
			Select(col).
			Where("wallpaper_id = ?", wpID).
			Scan(&cur).Error
		if err != nil {
			return false, cur, err
		}
		if cur >= want {
			return true, cur, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return cur == want, cur, nil
}

// runGroupCFeed 执行 (c) 关注流对照压测：纯 SQL 拉取（IN + ORDER BY）vs 推拉结合（Inbox + Outbox + 最大堆 K 路归并）。
func (s *benchSuite) runGroupCFeed(ctx context.Context) ([]BenchResult, error) {
	fmt.Println("=== [Group C] Following Feed Benchmarks (150 authors: 142 normal + 8 BigVs) ===")
	var results []BenchResult

	// 1. 预热全站大 V 集合 feed:bigvs
	bigVIDs, err := s.followRepo.ListAllBigVIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list bigv ids: %w", err)
	}
	if err := s.bigVSet.Reconcile(ctx, bigVIDs); err != nil {
		return nil, fmt.Errorf("reconcile bigv set: %w", err)
	}

	// 2. 预热推拉结合关注流（首调触发 Inbox 与 8 个大 V Outbox 懒重建），并比对结果与纯 SQL 完全一致
	sqlURL := fmt.Sprintf("%s/bench/v1/feed/following-sql", s.server.URL)
	sqlIDsURL := fmt.Sprintf("%s/bench/v1/feed/following-sql-ids", s.server.URL)
	hybridURL := fmt.Sprintf("%s/api/v1/feed/following?limit=20", s.server.URL)
	authHdr := map[string]string{"Authorization": s.readerToken}

	sqlIDs, err := s.fetchFeedItemIDs(ctx, sqlURL, nil)
	if err != nil {
		return nil, fmt.Errorf("probe sql following feed: %w", err)
	}
	hybridIDs, err := s.fetchFeedItemIDs(ctx, hybridURL, authHdr)
	if err != nil {
		return nil, fmt.Errorf("probe hybrid following feed: %w", err)
	}
	match := len(sqlIDs) == len(hybridIDs) && len(sqlIDs) == 20
	if match {
		for i := range sqlIDs {
			if sqlIDs[i] != hybridIDs[i] {
				match = false
				break
			}
		}
	}
	fmt.Printf("  [C-verify] Pure SQL vs Push-Pull Hybrid Top 20 IDs match=%v (count=%d)\n", match, len(hybridIDs))
	if !match {
		return nil, fmt.Errorf("following feed mismatch: sql=%v hybrid=%v", sqlIDs, hybridIDs)
	}

	// C1: 纯 SQL 拉取 ID（仅测 IN 150 作者 + ORDER BY id DESC LIMIT 21）
	rSQLIDs := runLoad(ctx, "C-关注流", "纯 SQL 拉取 ID (IN 150作者+ORDER BY)", 50, 2000, func(ctx context.Context, _ int) (int64, error) {
		return doHTTPRequest(ctx, s.client, http.MethodGet, sqlIDsURL, nil, http.StatusOK)
	})
	rSQLIDs.Extra = "仅拉取21条ID(无卡片装配)"
	results = append(results, rSQLIDs)
	fmt.Printf("  [C1] Pure SQL IDs Only (IN+ORDER BY): QPS=%.1f P50=%s P95=%s P99=%s\n",
		rSQLIDs.QPS, formatDurationMS(rSQLIDs.P50Latency), formatDurationMS(rSQLIDs.P95Latency), formatDurationMS(rSQLIDs.P99Latency))

	// C2: 纯 SQL 完整关注流（IN 150 作者 + ORDER BY + 直查 MySQL 装配 20 条卡片）
	rSQL := runLoad(ctx, "C-关注流", "纯 SQL 完整关注流 (IN+ORDER BY+直查装配)", 50, 2000, func(ctx context.Context, _ int) (int64, error) {
		return doHTTPRequest(ctx, s.client, http.MethodGet, sqlURL, nil, http.StatusOK)
	})
	rSQL.Extra = "142普通+8大V完整20条卡片"
	results = append(results, rSQL)
	fmt.Printf("  [C2] Pure SQL Full Feed: QPS=%.1f P50=%s P95=%s P99=%s\n",
		rSQL.QPS, formatDurationMS(rSQL.P50Latency), formatDurationMS(rSQL.P95Latency), formatDurationMS(rSQL.P99Latency))

	// C3: 生产推拉结合完整关注流（Inbox + 8 大 V Outbox + 最大堆 K 路归并 + 三级缓存批量装配）
	rHybrid := runLoad(ctx, "C-关注流", "推拉结合完整关注流 (Inbox+Outbox+K路归并)", 50, 5000, func(ctx context.Context, _ int) (int64, error) {
		return doHTTPRequest(ctx, s.client, http.MethodGet, hybridURL, authHdr, http.StatusOK)
	})
	rHybrid.Extra = fmt.Sprintf("vs纯SQL %.1fx (Top20一致)", rHybrid.QPS/max(rSQL.QPS, 1))
	results = append(results, rHybrid)
	fmt.Printf("  [C3] Push-Pull Hybrid Full Feed: QPS=%.1f P50=%s P95=%s P99=%s (%.1fx)\n",
		rHybrid.QPS, formatDurationMS(rHybrid.P50Latency), formatDurationMS(rHybrid.P95Latency), formatDurationMS(rHybrid.P99Latency),
		rHybrid.QPS/max(rSQL.QPS, 1))

	return results, nil
}

func (s *benchSuite) fetchFeedItemIDs(ctx context.Context, url string, headers map[string]string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d", resp.StatusCode)
	}
	var env struct {
		Code int `json:"code"`
		Data struct {
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(env.Data.Items))
	for _, it := range env.Data.Items {
		ids = append(ids, it.ID)
	}
	return ids, nil
}
