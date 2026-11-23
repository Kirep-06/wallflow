package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/service"
	"wallpaper-feed/internal/storage"
)

func TestFeedHandler_ListHotFeed_HTTP304AndCORSHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	l1 := gocache.NewCache(2 * 1024 * 1024)
	profileCache := cache.NewMultiLevel[model.UserProfileCache](l1, rdb, 30*time.Second, 30*time.Minute)
	wallpaperCache := cache.NewMultiLevel[model.WallpaperDetailCache](l1, rdb, 30*time.Second, 10*time.Minute)
	statsCache := cache.NewMultiLevel[model.WallpaperStatsCache](l1, rdb, 10*time.Second, 10*time.Minute)
	hotRank := cache.NewHotRank(rdb)
	store := storage.NewNoopStorage("http://cdn.local")

	userSvc := service.NewUserService(service.UserServiceDeps{
		ProfileCache: profileCache,
		Store:        store,
	})
	wpSvc := service.NewWallpaperService(
		nil, nil, userSvc, wallpaperCache, nil, store,
		service.WithWallpaperInteraction(nil, statsCache),
	)
	feedSvc := service.NewFeedService(service.FeedServiceDeps{
		HotRank:      hotRank,
		WallpaperSvc: wpSvc,
	})
	feedHandler := NewFeedHandler(feedSvc)

	r := gin.New()
	r.Use(middleware.CORS(true, nil))
	r.GET("/api/v1/feed/hot", feedHandler.ListHotFeed)

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	rawAuthor, _ := json.Marshal(model.UserProfileCache{ID: 1, Username: "alice", Nickname: "Alice"})
	_ = rdb.Set(ctx, "user:profile:1", rawAuthor, time.Minute).Err()
	rawWP, _ := json.Marshal(model.WallpaperDetailCache{
		ID: 501, UserID: 1, Title: "4K Sunset", ObjectKey: "wallpapers/1/501.jpg", CreatedAt: now,
	})
	_ = rdb.Set(ctx, "wallpaper:detail:501", rawWP, time.Minute).Err()
	rawStats, _ := json.Marshal(model.WallpaperStatsCache{LikeCount: 12, DownloadCount: 5})
	_ = rdb.Set(ctx, cache.WallpaperStatsKey(501), rawStats, time.Minute).Err()

	// 1. Redis 未就绪降级：不发 ETag，返回 Cache-Control: private, no-store
	reqFallback := httptest.NewRequest(http.MethodGet, "/api/v1/feed/hot?limit=10", nil)
	recFallback := httptest.NewRecorder()
	r.ServeHTTP(recFallback, reqFallback)
	if recFallback.Code != http.StatusOK {
		t.Fatalf("fallback status = %d, want 200", recFallback.Code)
	}
	if got := recFallback.Header().Get("ETag"); got != "" {
		t.Fatalf("fallback must not emit ETag, got %q", got)
	}
	if got := recFallback.Header().Get("Cache-Control"); got != "private, no-store" {
		t.Fatalf("fallback Cache-Control = %q, want private, no-store", got)
	}

	// 2. 写入热榜快照后首屏请求：200 + ETag + CORS 放行/暴露头 + Vary 同时保留 Origin 与 Authorization
	ver, err := hotRank.Swap(ctx, []model.HotRankEntry{{WallpaperID: 501, Score: 12.5}})
	if err != nil {
		t.Fatalf("Swap failed: %v", err)
	}
	wantETag := `W/"hot-` + ver + `-l10-u0"`

	req1 := httptest.NewRequest(http.MethodGet, "/api/v1/feed/hot?limit=10", nil)
	req1.Header.Set("Origin", "http://localhost:5173")
	rec1 := httptest.NewRecorder()
	r.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want 200, body=%s", rec1.Code, rec1.Body.String())
	}
	if got := rec1.Header().Get("ETag"); got != wantETag {
		t.Fatalf("ETag = %q, want %q", got, wantETag)
	}
	if got := rec1.Header().Get("Cache-Control"); got != "private, no-cache" {
		t.Fatalf("Cache-Control = %q, want private, no-cache", got)
	}
	if got := rec1.Header().Get("Access-Control-Expose-Headers"); got != "ETag" {
		t.Fatalf("Access-Control-Expose-Headers = %q, want ETag", got)
	}
	if got := rec1.Header().Get("Access-Control-Allow-Headers"); !strings.Contains(got, "If-None-Match") {
		t.Fatalf("Access-Control-Allow-Headers = %q, must include If-None-Match", got)
	}
	varies := rec1.Header().Values("Vary")
	if !slices.Contains(varies, "Origin") || !slices.Contains(varies, "Authorization") {
		t.Fatalf("Vary headers = %v, want both Origin and Authorization", varies)
	}

	// 3. 携带 If-None-Match 命中 304：严格空响应体且保留 ETag / Cache-Control / Vary
	req304 := httptest.NewRequest(http.MethodGet, "/api/v1/feed/hot?limit=10", nil)
	req304.Header.Set("If-None-Match", wantETag)
	rec304 := httptest.NewRecorder()
	r.ServeHTTP(rec304, req304)

	if rec304.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified, got %d body=%s", rec304.Code, rec304.Body.String())
	}
	if rec304.Body.Len() != 0 {
		t.Fatalf("304 response must have empty body, got %q", rec304.Body.String())
	}
	if got := rec304.Header().Get("ETag"); got != wantETag {
		t.Fatalf("304 ETag = %q, want %q", got, wantETag)
	}
	if got := rec304.Header().Get("Cache-Control"); got != "private, no-cache" {
		t.Fatalf("304 Cache-Control = %q, want private, no-cache", got)
	}

	// 4. 非法游标返回 400 / 50001
	reqBad := httptest.NewRequest(http.MethodGet, "/api/v1/feed/hot?cursor=not-valid", nil)
	recBad := httptest.NewRecorder()
	r.ServeHTTP(recBad, reqBad)
	if recBad.Code != http.StatusBadRequest {
		t.Fatalf("invalid cursor status = %d, want 400", recBad.Code)
	}
	var env struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(recBad.Body.Bytes(), &env); err != nil || env.Code != 50001 {
		t.Fatalf("invalid cursor body = %s, want code=50001", recBad.Body.String())
	}
}
