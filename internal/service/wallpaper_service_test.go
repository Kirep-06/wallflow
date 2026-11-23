package service

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
)

func TestNormalizeAndValidateTags(t *testing.T) {
	t.Run("nil or empty returns empty non-nil slice", func(t *testing.T) {
		got, err := normalizeAndValidateTags(nil)
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("normalizeAndValidateTags(nil) = (%v, %v)", got, err)
		}
	})

	t.Run("trim lowercase and deduplicate keeping first order", func(t *testing.T) {
		got, err := normalizeAndValidateTags([]string{" 4K ", "赛博朋克", "4k", "夜景", "赛博朋克"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"4k", "赛博朋克", "夜景"}
		if !slices.Equal(got, want) {
			t.Fatalf("got = %v, want %v", got, want)
		}
	})

	t.Run("exact 24 chinese runes allowed", func(t *testing.T) {
		tag24 := strings.Repeat("画", 24)
		got, err := normalizeAndValidateTags([]string{tag24})
		if err != nil || len(got) != 1 || got[0] != tag24 {
			t.Fatalf("got = (%v, %v)", got, err)
		}
	})

	t.Run("invalid tags return 30003", func(t *testing.T) {
		tests := []struct {
			name string
			tags []string
		}{
			{name: "more than 5 tags even with duplicates", tags: []string{"a", "b", "c", "d", "e", "a"}},
			{name: "blank tag", tags: []string{"   "}},
			{name: "over 24 runes", tags: []string{strings.Repeat("a", 25)}},
			{name: "internal space", tags: []string{"cyber punk"}},
			{name: "internal tab", tags: []string{"cyber\tpunk"}},
			{name: "ascii comma", tags: []string{"a,b"}},
			{name: "fullwidth comma", tags: []string{"动漫，夜景"}},
			{name: "control char", tags: []string{"abc\x00def"}},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := normalizeAndValidateTags(tc.tags); !errors.Is(err, errno.ErrInvalidCategoryOrTag) {
					t.Fatalf("err = %v, want ErrInvalidCategoryOrTag", err)
				}
			})
		}
	})
}

func TestEscapeLikePrefix(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "cyber", want: "cyber%"},
		{in: "100%_a\\b", want: `100\%\_a\\b%`},
		{in: `\\%_`, want: `\\\\\%\_%`},
	}

	for _, tc := range tests {
		if got := escapeLikePrefix(tc.in); got != tc.want {
			t.Fatalf("escapeLikePrefix(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseCategoryID(t *testing.T) {
	validTests := []struct {
		in   string
		want uint64
	}{
		{in: "1", want: 1},
		{in: "10001", want: 10001},
	}
	for _, tc := range validTests {
		got, err := parseCategoryID(tc.in)
		if err != nil || got != tc.want {
			t.Fatalf("parseCategoryID(%q) = (%d, %v), want (%d, nil)", tc.in, got, err, tc.want)
		}
	}

	invalidInputs := []string{"", "0", "01", "-1", " 1 ", "1a", "999999999999999999999"}
	for _, in := range invalidInputs {
		if _, err := parseCategoryID(in); !errors.Is(err, errno.ErrInvalidParam) {
			t.Fatalf("parseCategoryID(%q) err = %v, want ErrInvalidParam", in, err)
		}
	}
}

func TestWallpaperServicePreDBValidation(t *testing.T) {
	svc := NewWallpaperService(nil, nil, nil, nil, nil, &stubStorage{})
	validKey := "wallpapers/10/20261122/9f3c2e8a4b1d4a7c8e5f1a2b3c4d5e6f.webp"

	tests := []struct {
		name    string
		req     model.CreateWallpaperRequest
		wantErr error
	}{
		{
			name:    "empty title",
			req:     model.CreateWallpaperRequest{Title: "   ", CategoryID: "1", ObjectKey: validKey},
			wantErr: errno.ErrInvalidParam,
		},
		{
			name:    "title over 80 runes",
			req:     model.CreateWallpaperRequest{Title: strings.Repeat("星", 81), CategoryID: "1", ObjectKey: validKey},
			wantErr: errno.ErrInvalidParam,
		},
		{
			name:    "description over 500 runes",
			req:     model.CreateWallpaperRequest{Title: "ok", Description: strings.Repeat("字", 501), CategoryID: "1", ObjectKey: validKey},
			wantErr: errno.ErrInvalidParam,
		},
		{
			name:    "invalid category_id format",
			req:     model.CreateWallpaperRequest{Title: "ok", CategoryID: "01", ObjectKey: validKey},
			wantErr: errno.ErrInvalidParam,
		},
		{
			name:    "invalid tags before db",
			req:     model.CreateWallpaperRequest{Title: "ok", CategoryID: "1", Tags: []string{"bad tag"}, ObjectKey: validKey},
			wantErr: errno.ErrInvalidCategoryOrTag,
		},
		{
			name:    "foreign object_key before db",
			req:     model.CreateWallpaperRequest{Title: "ok", CategoryID: "1", ObjectKey: "wallpapers/99/20261122/9f3c2e8a4b1d4a7c8e5f1a2b3c4d5e6f.webp"},
			wantErr: errno.ErrInvalidParam,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := svc.CreateWallpaper(context.Background(), 10, tc.req); !errors.Is(err, tc.wantErr) {
				t.Fatalf("CreateWallpaper err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestWallpaperDetailL2TTL(t *testing.T) {
	now := time.Now().UTC()
	if got := WallpaperDetailL2TTL(model.WallpaperDetailCache{CreatedAt: now.Add(-24 * time.Hour)}); got != wallpaperDetailHotL2TTL {
		t.Fatalf("recent wallpaper TTL = %v, want %v", got, wallpaperDetailHotL2TTL)
	}
	if got := WallpaperDetailL2TTL(model.WallpaperDetailCache{CreatedAt: now.Add(-8 * 24 * time.Hour)}); got != wallpaperDetailColdL2TTL {
		t.Fatalf("old wallpaper TTL = %v, want %v", got, wallpaperDetailColdL2TTL)
	}
	if got := WallpaperDetailL2TTL(model.WallpaperDetailCache{}); got != wallpaperDetailColdL2TTL {
		t.Fatalf("zero time wallpaper TTL = %v, want %v", got, wallpaperDetailColdL2TTL)
	}
}

func TestListWallpapersValidation(t *testing.T) {
	svc := NewWallpaperService(nil, nil, nil, nil, nil, &stubStorage{})
	ctx := context.Background()

	// category_id 与 tag_id 单维互斥，同时传入返回 10002。
	if _, err := svc.ListWallpapers(ctx, model.WallpaperListQuery{
		CategoryID: 1,
		TagID:      2,
		Limit:      20,
	}); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("both category_id and tag_id err = %v, want ErrInvalidParam", err)
	}

	// limit 越界返回 10002。
	if _, err := svc.ListWallpapers(ctx, model.WallpaperListQuery{Limit: 51}); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("limit=51 err = %v, want ErrInvalidParam", err)
	}

	// 篡改游标或 id=0 游标返回 50001。
	zeroCursor, err := cursor.Encode(cursor.IDCursor{ID: 0})
	if err != nil {
		t.Fatalf("encode zero cursor: %v", err)
	}
	for _, bad := range []string{"not-base64!!!", zeroCursor} {
		if _, err := svc.ListWallpapers(ctx, model.WallpaperListQuery{Cursor: bad, Limit: 20}); !errors.Is(err, errno.ErrInvalidCursor) {
			t.Fatalf("bad cursor %q err = %v, want ErrInvalidCursor", bad, err)
		}
	}
}

func TestPaginateIDs(t *testing.T) {
	pageIDs, next, hasMore, err := paginateIDs([]uint64{10, 9, 8}, 2)
	if err != nil || !hasMore || !slices.Equal(pageIDs, []uint64{10, 9}) {
		t.Fatalf("paginateIDs(hasMore) = (%v, %q, %v, %v)", pageIDs, next, hasMore, err)
	}
	decoded, err := cursor.Decode[cursor.IDCursor](next)
	if err != nil || decoded.ID != 9 {
		t.Fatalf("decoded next_cursor = (%+v, %v), want ID=9", decoded, err)
	}

	pageIDs, next, hasMore, err = paginateIDs([]uint64{10, 9}, 2)
	if err != nil || hasMore || next != "" || !slices.Equal(pageIDs, []uint64{10, 9}) {
		t.Fatalf("paginateIDs(exact) = (%v, %q, %v, %v)", pageIDs, next, hasMore, err)
	}
}

func TestFetchLatestIDsWithFallback(t *testing.T) {
	t.Run("pure hot hit skips cold query", func(t *testing.T) {
		coldCalled := false
		ids, err := fetchLatestIDsWithFallback(
			0, 3,
			func(uint64, int) ([]uint64, bool, error) { return []uint64{30, 29, 28}, true, nil },
			func(uint64, int) ([]uint64, error) {
				coldCalled = true
				return nil, nil
			},
		)
		if err != nil || coldCalled || !slices.Equal(ids, []uint64{30, 29, 28}) {
			t.Fatalf("pure hot = (%v, %v, coldCalled=%v)", ids, err, coldCalled)
		}
	})

	t.Run("hot-cold boundary seamlessly backfills remaining from last hot ID", func(t *testing.T) {
		var gotBefore uint64
		var gotNeed int
		ids, err := fetchLatestIDsWithFallback(
			100, 4,
			func(uint64, int) ([]uint64, bool, error) { return []uint64{95, 90}, true, nil },
			func(before uint64, need int) ([]uint64, error) {
				gotBefore, gotNeed = before, need
				return []uint64{88, 85}, nil
			},
		)
		if err != nil || gotBefore != 90 || gotNeed != 2 || !slices.Equal(ids, []uint64{95, 90, 88, 85}) {
			t.Fatalf("boundary = (%v, err=%v, before=%d, need=%d)", ids, err, gotBefore, gotNeed)
		}
	})

	t.Run("deep cold page uses original cursor beforeID", func(t *testing.T) {
		var gotBefore uint64
		var gotNeed int
		ids, err := fetchLatestIDsWithFallback(
			50, 3,
			func(uint64, int) ([]uint64, bool, error) { return []uint64{}, true, nil },
			func(before uint64, need int) ([]uint64, error) {
				gotBefore, gotNeed = before, need
				return []uint64{49, 48}, nil
			},
		)
		if err != nil || gotBefore != 50 || gotNeed != 3 || !slices.Equal(ids, []uint64{49, 48}) {
			t.Fatalf("deep cold = (%v, err=%v, before=%d, need=%d)", ids, err, gotBefore, gotNeed)
		}
	})

	t.Run("not ready falls back directly to cold", func(t *testing.T) {
		ids, err := fetchLatestIDsWithFallback(
			0, 2,
			func(uint64, int) ([]uint64, bool, error) { return nil, false, nil },
			func(uint64, int) ([]uint64, error) { return []uint64{5, 4}, nil },
		)
		if err != nil || !slices.Equal(ids, []uint64{5, 4}) {
			t.Fatalf("not ready fallback = (%v, %v)", ids, err)
		}
	})
}

func TestAssembleSummaryPage_SkipsGhostIDsAndFallbackAuthor(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	l1 := gocache.NewCache(2 * 1024 * 1024)
	profileCache := cache.NewMultiLevel[model.UserProfileCache](l1, rdb, 30*time.Second, 30*time.Minute)
	wallpaperCache := cache.NewMultiLevel(l1, rdb, 30*time.Second, 10*time.Minute, cache.WithL2TTLFunc(WallpaperDetailL2TTL))
	store := &stubStorage{}
	userSvc := NewUserService(UserServiceDeps{
		ProfileCache: profileCache,
		Store:        store,
	})
	svc := NewWallpaperService(nil, nil, userSvc, wallpaperCache, nil, store)

	// 预灌缓存：壁纸 10（正常作者 1）、壁纸 9（幽灵空值哨兵）、壁纸 8（作者 2 缺失空值哨兵）。
	wp10, _ := json.Marshal(model.WallpaperDetailCache{
		ID: 10, UserID: 1, CategoryID: 3, Title: "neon", ObjectKey: "wallpapers/1/20261122/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp",
		Width: 3840, Height: 2160, Orientation: 1, Is4K: true, Category: model.CategoryItem{ID: 3, Slug: "cyberpunk", Name: "赛博科幻"},
		CreatedAt: time.Now().UTC(),
	})
	wp8, _ := json.Marshal(model.WallpaperDetailCache{
		ID: 8, UserID: 2, CategoryID: 3, Title: "rain", ObjectKey: "wallpapers/2/20261122/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.webp",
		Width: 1920, Height: 1080, Orientation: 1, Is4K: false, Category: model.CategoryItem{ID: 3, Slug: "cyberpunk", Name: "赛博科幻"},
		CreatedAt: time.Now().UTC(),
	})
	u1, _ := json.Marshal(model.UserProfileCache{ID: 1, Username: "alice", Nickname: "爱丽丝", AvatarKey: "avatars/1/20261122/cccccccccccccccccccccccccccccccc.png"})

	_ = mr.Set("wallpaper:detail:10", string(wp10))
	_ = mr.Set("wallpaper:detail:9", cache.NilMarker)
	_ = mr.Set("wallpaper:detail:8", string(wp8))
	_ = mr.Set("user:profile:1", string(u1))
	_ = mr.Set("user:profile:2", cache.NilMarker)

	page, err := svc.assembleSummaryPage(context.Background(), []uint64{10, 9, 8}, "next-cur", true, nil, nil)
	if err != nil {
		t.Fatalf("assembleSummaryPage failed: %v", err)
	}
	if !page.HasMore || page.NextCursor != "next-cur" {
		t.Fatalf("pagination metadata mismatch: %+v", page)
	}
	if len(page.Items) != 2 || page.Items[0].ID != 10 || page.Items[1].ID != 8 {
		t.Fatalf("expected [10, 8] skipping ghost 9, got %+v", page.Items)
	}
	if page.Items[0].Author.Nickname != "爱丽丝" {
		t.Fatalf("item[0] author = %+v", page.Items[0].Author)
	}
	// 作者 2 异常缺失时回退为安全占位而不是拖垮整页。
	if page.Items[1].Author.ID != 2 || !strings.Contains(page.Items[1].Author.AvatarURL, defaultAvatarKey) {
		t.Fatalf("item[1] fallback author = %+v", page.Items[1].Author)
	}
}
