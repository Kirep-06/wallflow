package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/mq"
	"wallpaper-feed/internal/pkg/errno"
)

type stubLightPublisher struct {
	err  error
	msgs []mq.PublishMessage
}

func (p *stubLightPublisher) PublishLight(_ context.Context, msg mq.PublishMessage) error {
	p.msgs = append(p.msgs, msg)
	return p.err
}

func TestValidateCommentContent(t *testing.T) {
	t.Run("trims spaces and accepts exact 500 runes", func(t *testing.T) {
		got, err := validateCommentContent("  光影绝美！ \n ")
		if err != nil || got != "光影绝美！" {
			t.Fatalf("got = (%q, %v)", got, err)
		}

		c500 := strings.Repeat("画", 500)
		got, err = validateCommentContent(c500)
		if err != nil || got != c500 {
			t.Fatalf("500 runes expected ok, got err=%v", err)
		}
	})

	t.Run("empty blank or over 500 runes returns 60002", func(t *testing.T) {
		for _, in := range []string{"", "   \t\n  ", strings.Repeat("字", 501)} {
			if _, err := validateCommentContent(in); !errors.Is(err, errno.ErrInvalidComment) {
				t.Fatalf("validateCommentContent(%q) err = %v, want ErrInvalidComment", in, err)
			}
		}
	})
}

func TestStatsDeltaAddAndIsZero(t *testing.T) {
	var d model.StatsDelta
	if !d.IsZero() {
		t.Fatalf("zero delta should report IsZero")
	}
	if !d.Add(model.InteractionActionLike, 1) || !d.Add(model.InteractionActionLike, -1) {
		t.Fatalf("like +1/-1 should be valid")
	}
	if !d.IsZero() {
		t.Fatalf("like +1 and -1 should cancel to zero: %+v", d)
	}
	if d.Add(model.InteractionActionDownload, -1) {
		t.Fatalf("download -1 must be rejected")
	}
	if d.Add(model.InteractionActionFavorite, 2) || d.Add("unknown", 1) {
		t.Fatalf("invalid delta/action must be rejected")
	}
	if !d.Add(model.InteractionActionDownload, 1) || !d.Add(model.InteractionActionComment, -1) {
		t.Fatalf("download +1 and comment -1 should be valid")
	}
	if d.IsZero() || d.Download != 1 || d.Comment != -1 {
		t.Fatalf("unexpected delta state: %+v", d)
	}
}

func TestWallpaperService_LightsUpStatsFromMultiLevelCache(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	l1 := gocache.NewCache(2 * 1024 * 1024)
	profileCache := cache.NewMultiLevel[model.UserProfileCache](l1, rdb, 30*time.Second, 30*time.Minute)
	wallpaperCache := cache.NewMultiLevel[model.WallpaperDetailCache](l1, rdb, 30*time.Second, 10*time.Minute)
	statsCache := cache.NewMultiLevel[model.WallpaperStatsCache](l1, rdb, 10*time.Second, 10*time.Minute)

	store := &stubStorage{}
	userSvc := NewUserService(UserServiceDeps{ProfileCache: profileCache, Store: store})
	wpSvc := NewWallpaperService(nil, nil, userSvc, wallpaperCache, nil, store, WithWallpaperInteraction(nil, statsCache))

	wp1, _ := json.Marshal(model.WallpaperDetailCache{
		ID: 101, UserID: 1, Title: "hot", ObjectKey: "wallpapers/1/20261123/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp",
		CreatedAt: time.Now().UTC(),
	})
	wp2, _ := json.Marshal(model.WallpaperDetailCache{
		ID: 102, UserID: 1, Title: "cold", ObjectKey: "wallpapers/1/20261123/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.webp",
		CreatedAt: time.Now().UTC(),
	})
	u1, _ := json.Marshal(model.UserProfileCache{ID: 1, Username: "alice", Nickname: "爱丽丝"})
	st1, _ := json.Marshal(model.WallpaperStatsCache{LikeCount: 12, FavCount: 5, DownloadCount: 88, CommentCount: 3})

	_ = mr.Set(wallpaperDetailKey(101), string(wp1))
	_ = mr.Set(wallpaperDetailKey(102), string(wp2))
	_ = mr.Set(userProfileKey(1), string(u1))
	_ = mr.Set(cache.WallpaperStatsKey(101), string(st1))

	ctx := context.Background()
	detail, err := wpSvc.GetWallpaperDetail(ctx, 101, 0)
	if err != nil {
		t.Fatalf("GetWallpaperDetail failed: %v", err)
	}
	if detail.LikeCount != 12 || detail.FavCount != 5 || detail.DownloadCount != 88 || detail.CommentCount != 3 {
		t.Fatalf("detail stats mismatch: %+v", detail)
	}

	page, err := wpSvc.assembleSummaryPage(ctx, []uint64{101, 102}, "", false, summaryAssembleOpts{})
	if err != nil {
		t.Fatalf("assembleSummaryPage failed: %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(page.Items))
	}
	if page.Items[0].LikeCount != 12 || page.Items[0].DownloadCount != 88 {
		t.Fatalf("item[0] stats mismatch: %+v", page.Items[0])
	}
	// 冷图 102 无 stats 行时自动回落全零结构体。
	if page.Items[1].LikeCount != 0 || page.Items[1].FavCount != 0 || page.Items[1].DownloadCount != 0 || page.Items[1].CommentCount != 0 {
		t.Fatalf("item[1] cold stats should be zero: %+v", page.Items[1])
	}
}

func TestInteractionService_RecordDownloadAndMQDegrade(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	l1 := gocache.NewCache(2 * 1024 * 1024)
	wallpaperCache := cache.NewMultiLevel[model.WallpaperDetailCache](l1, rdb, 30*time.Second, 10*time.Minute)
	wp1, _ := json.Marshal(model.WallpaperDetailCache{
		ID: 55, UserID: 1, Title: "sunset", ObjectKey: "wallpapers/1/20261123/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp",
		CreatedAt: time.Now().UTC(),
	})
	_ = mr.Set(wallpaperDetailKey(55), string(wp1))
	_ = mr.Set(wallpaperDetailKey(404), cache.NilMarker)

	wpSvc := NewWallpaperService(nil, nil, nil, wallpaperCache, nil, &stubStorage{})
	ctx := context.Background()

	t.Run("not found wallpaper returns 30002", func(t *testing.T) {
		pub := &stubLightPublisher{}
		svc := NewInteractionService(nil, wpSvc, nil, pub)
		if err := svc.RecordDownload(ctx, 9, 404); !errors.Is(err, errno.ErrWallpaperNotFound) {
			t.Fatalf("err = %v, want ErrWallpaperNotFound", err)
		}
		if len(pub.msgs) != 0 {
			t.Fatalf("must not publish event for missing wallpaper")
		}
	})

	t.Run("publishes interaction event payload to wallpaper.interaction", func(t *testing.T) {
		pub := &stubLightPublisher{}
		svc := NewInteractionService(nil, wpSvc, nil, pub)
		if err := svc.RecordDownload(ctx, 42, 55); err != nil {
			t.Fatalf("RecordDownload failed: %v", err)
		}
		if len(pub.msgs) != 1 {
			t.Fatalf("expected 1 published message, got %d", len(pub.msgs))
		}
		msg := pub.msgs[0]
		if msg.Exchange != mq.ExchangeEvents || msg.RoutingKey != mq.RoutingKeyWallpaperInteraction {
			t.Fatalf("unexpected routing: %s/%s", msg.Exchange, msg.RoutingKey)
		}
		var ev model.InteractionEvent
		if err := json.Unmarshal(msg.Body, &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		if ev.WallpaperID != 55 || ev.UserID != 42 || ev.Action != model.InteractionActionDownload || ev.Delta != 1 {
			t.Fatalf("unexpected event payload: %+v", ev)
		}
	})

	t.Run("mq down or nil publisher degrades gracefully without error", func(t *testing.T) {
		svcDegraded := NewInteractionService(nil, wpSvc, nil, &stubLightPublisher{err: mq.ErrNotConnected})
		if err := svcDegraded.RecordDownload(ctx, 0, 55); err != nil {
			t.Fatalf("mq down must degrade gracefully, got %v", err)
		}

		svcNil := NewInteractionService(nil, wpSvc, nil, nil)
		if err := svcNil.RecordDownload(ctx, 0, 55); err != nil {
			t.Fatalf("nil publisher must degrade gracefully, got %v", err)
		}
	})
}
