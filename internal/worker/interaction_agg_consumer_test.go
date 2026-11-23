package worker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/mq"
)

type stubStatsFlusher struct {
	mu           sync.Mutex
	failCount    int
	failErr      error
	calls        [][]model.StatsDelta
	lastCtxAlive bool
}

func (f *stubStatsFlusher) BatchApplyStatsDeltas(ctx context.Context, deltas []model.StatsDelta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastCtxAlive = ctx.Err() == nil
	if f.failCount > 0 {
		f.failCount--
		return f.failErr
	}
	cp := slices.Clone(deltas)
	slices.SortFunc(cp, func(a, b model.StatsDelta) int {
		return cmp.Compare(a.WallpaperID, b.WallpaperID)
	})
	f.calls = append(f.calls, cp)
	return nil
}

func (f *stubStatsFlusher) snapshotCalls() [][]model.StatsDelta {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]model.StatsDelta, len(f.calls))
	copy(out, f.calls)
	return out
}

type mockAckTracker struct {
	mu        sync.Mutex
	acked     []bool
	nacked    [][2]bool
	ackedTags []int
}

func makeStreamDelivery(tag int, body []byte, tracker *mockAckTracker) mq.StreamDelivery {
	return mq.StreamDelivery{
		Body:      body,
		MessageID: "msg",
		Ack: func(multiple bool) error {
			tracker.mu.Lock()
			defer tracker.mu.Unlock()
			tracker.acked = append(tracker.acked, multiple)
			tracker.ackedTags = append(tracker.ackedTags, tag)
			return nil
		},
		Nack: func(multiple, requeue bool) error {
			tracker.mu.Lock()
			defer tracker.mu.Unlock()
			tracker.nacked = append(tracker.nacked, [2]bool{multiple, requeue})
			return nil
		},
	}
}

func mustEventBytes(t *testing.T, wpID, uid uint64, action string, delta int64) []byte {
	t.Helper()
	b, err := json.Marshal(model.NewInteractionEvent(wpID, uid, action, delta, time.Now()))
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return b
}

func TestInteractionAggConsumer_BatchFlushAndCacheUnlink(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	_ = mr.Set(cache.WallpaperStatsKey(10), `{"like_count":1}`)
	_ = mr.Set(cache.WallpaperStatsKey(20), `{"like_count":2}`)

	flusher := &stubStatsFlusher{}
	consumer := &InteractionAggConsumer{
		flusher:        flusher,
		rdb:            rdb,
		batchSize:      4,
		flushInterval:  time.Hour,
		retryBaseDelay: time.Millisecond,
	}

	tracker := &mockAckTracker{}
	deliveries := make(chan mq.StreamDelivery, 4)
	deliveries <- makeStreamDelivery(1, mustEventBytes(t, 20, 1, model.InteractionActionLike, 1), tracker)
	deliveries <- makeStreamDelivery(2, mustEventBytes(t, 10, 2, model.InteractionActionDownload, 1), tracker)
	deliveries <- makeStreamDelivery(3, mustEventBytes(t, 20, 3, model.InteractionActionFavorite, 1), tracker)
	deliveries <- makeStreamDelivery(4, mustEventBytes(t, 20, 4, model.InteractionActionComment, 1), tracker)
	close(deliveries)

	if err := consumer.runSession(context.Background(), deliveries); err != nil {
		t.Fatalf("runSession failed: %v", err)
	}

	calls := flusher.snapshotCalls()
	if len(calls) != 1 || len(calls[0]) != 2 {
		t.Fatalf("expected 1 batch flush with 2 wallpapers, got %+v", calls)
	}
	if calls[0][0] != (model.StatsDelta{WallpaperID: 10, Download: 1}) {
		t.Fatalf("wp 10 delta mismatch: %+v", calls[0][0])
	}
	if calls[0][1] != (model.StatsDelta{WallpaperID: 20, Like: 1, Fav: 1, Comment: 1}) {
		t.Fatalf("wp 20 delta mismatch: %+v", calls[0][1])
	}

	// 仅对本批次末条消息执行 1 次批量 Ack(true)。
	if len(tracker.acked) != 1 || !tracker.acked[0] || tracker.ackedTags[0] != 4 {
		t.Fatalf("expected single batch Ack(true) on tag 4, got acked=%v tags=%v", tracker.acked, tracker.ackedTags)
	}
	// 对应 L2 缓存应已被批量 UNLINK。
	if mr.Exists(cache.WallpaperStatsKey(10)) || mr.Exists(cache.WallpaperStatsKey(20)) {
		t.Fatalf("expected L2 wallpaper:stats keys unlinked after flush")
	}
}

func TestInteractionAggConsumer_TimerFlushAndZeroCancelSkipDB(t *testing.T) {
	flusher := &stubStatsFlusher{}
	consumer := &InteractionAggConsumer{
		flusher:        flusher,
		batchSize:      200,
		flushInterval:  25 * time.Millisecond,
		retryBaseDelay: time.Millisecond,
	}

	tracker := &mockAckTracker{}
	deliveries := make(chan mq.StreamDelivery, 4)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.runSession(ctx, deliveries)
	}()

	// 同窗 +1 与 -1 互相抵消：定时器触发时应跳过 DB 写，但仍批量 Ack(true)。
	deliveries <- makeStreamDelivery(1, mustEventBytes(t, 100, 1, model.InteractionActionLike, 1), tracker)
	deliveries <- makeStreamDelivery(2, mustEventBytes(t, 100, 1, model.InteractionActionLike, -1), tracker)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		tracker.mu.Lock()
		n := len(tracker.acked)
		tracker.mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	close(deliveries)
	if err := <-done; err != nil {
		t.Fatalf("runSession failed: %v", err)
	}

	if len(flusher.snapshotCalls()) != 0 {
		t.Fatalf("expected 0 DB writes for zero-sum delta, got %+v", flusher.snapshotCalls())
	}
	if len(tracker.acked) != 1 || !tracker.acked[0] || tracker.ackedTags[0] != 2 {
		t.Fatalf("expected batch Ack(true) on tag 2, got acked=%v tags=%v", tracker.acked, tracker.ackedTags)
	}
}

func TestInteractionAggConsumer_PoisonPillIsolatedToDLQ(t *testing.T) {
	flusher := &stubStatsFlusher{}
	consumer := &InteractionAggConsumer{
		flusher:        flusher,
		batchSize:      2,
		flushInterval:  time.Hour,
		retryBaseDelay: time.Millisecond,
	}

	tracker := &mockAckTracker{}
	deliveries := make(chan mq.StreamDelivery, 5)
	deliveries <- makeStreamDelivery(1, mustEventBytes(t, 50, 1, model.InteractionActionLike, 1), tracker)
	deliveries <- makeStreamDelivery(2, []byte("{bad-json"), tracker)
	deliveries <- makeStreamDelivery(3, mustEventBytes(t, 50, 1, model.InteractionActionDownload, -1), tracker)
	deliveries <- makeStreamDelivery(4, mustEventBytes(t, 0, 1, model.InteractionActionLike, 1), tracker)
	deliveries <- makeStreamDelivery(5, mustEventBytes(t, 50, 2, model.InteractionActionLike, 1), tracker)
	close(deliveries)

	if err := consumer.runSession(context.Background(), deliveries); err != nil {
		t.Fatalf("runSession failed: %v", err)
	}

	if len(tracker.nacked) != 3 {
		t.Fatalf("expected 3 poison pills nacked, got %+v", tracker.nacked)
	}
	for i, n := range tracker.nacked {
		if n != [2]bool{false, false} {
			t.Fatalf("poison pill %d expected Nack(false, false), got %v", i, n)
		}
	}
	calls := flusher.snapshotCalls()
	if len(calls) != 1 || calls[0][0].Like != 2 {
		t.Fatalf("expected 1 batch flush with like=+2, got %+v", calls)
	}
	if len(tracker.acked) != 1 || tracker.ackedTags[0] != 5 {
		t.Fatalf("expected batch Ack(true) on tag 5, got tags=%v", tracker.ackedTags)
	}
}

func TestInteractionAggConsumer_RetryNeverAckOnFailureAndAckOnRedisError(t *testing.T) {
	t.Run("transient db failure retries and succeeds with ack", func(t *testing.T) {
		flusher := &stubStatsFlusher{failCount: 2, failErr: errors.New("mysql transient")}
		consumer := &InteractionAggConsumer{
			flusher:        flusher,
			batchSize:      1,
			flushInterval:  time.Hour,
			retryBaseDelay: time.Millisecond,
		}
		tracker := &mockAckTracker{}
		deliveries := make(chan mq.StreamDelivery, 1)
		deliveries <- makeStreamDelivery(1, mustEventBytes(t, 7, 1, model.InteractionActionDownload, 1), tracker)
		close(deliveries)

		if err := consumer.runSession(context.Background(), deliveries); err != nil {
			t.Fatalf("expected retry success, got %v", err)
		}
		if len(tracker.acked) != 1 || !tracker.acked[0] {
			t.Fatalf("expected Ack(true) after retry success, got %v", tracker.acked)
		}
	})

	t.Run("persistent db failure must not ack", func(t *testing.T) {
		flusher := &stubStatsFlusher{failCount: 10, failErr: errors.New("mysql down")}
		consumer := &InteractionAggConsumer{
			flusher:        flusher,
			batchSize:      1,
			flushInterval:  time.Hour,
			retryBaseDelay: time.Millisecond,
		}
		tracker := &mockAckTracker{}
		deliveries := make(chan mq.StreamDelivery, 1)
		deliveries <- makeStreamDelivery(1, mustEventBytes(t, 7, 1, model.InteractionActionDownload, 1), tracker)
		close(deliveries)

		if err := consumer.runSession(context.Background(), deliveries); err == nil {
			t.Fatalf("expected error on persistent db failure")
		}
		if len(tracker.acked) != 0 {
			t.Fatalf("must NOT ack when db flush failed, got %v", tracker.acked)
		}
	})

	t.Run("redis unlink error after db success still acks to prevent double count", func(t *testing.T) {
		mr := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		mr.Close() // 模拟 Redis 断连

		flusher := &stubStatsFlusher{}
		consumer := &InteractionAggConsumer{
			flusher:        flusher,
			rdb:            rdb,
			batchSize:      1,
			flushInterval:  time.Hour,
			retryBaseDelay: time.Millisecond,
		}
		tracker := &mockAckTracker{}
		deliveries := make(chan mq.StreamDelivery, 1)
		deliveries <- makeStreamDelivery(1, mustEventBytes(t, 8, 1, model.InteractionActionLike, 1), tracker)
		close(deliveries)

		if err := consumer.runSession(context.Background(), deliveries); err != nil {
			t.Fatalf("expected success despite redis unlink err, got %v", err)
		}
		if len(tracker.acked) != 1 || !tracker.acked[0] {
			t.Fatalf("expected Ack(true) after db persisted, got %v", tracker.acked)
		}
	})
}

func TestInteractionAggConsumer_GracefulShutdownZeroLossVsUnexpectedDisconnect(t *testing.T) {
	t.Run("graceful shutdown flushes partial batch with WithoutCancel context", func(t *testing.T) {
		mr := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		_ = mr.Set(cache.WallpaperStatsKey(99), `{"like_count":0}`)

		flusher := &stubStatsFlusher{}
		consumer := &InteractionAggConsumer{
			flusher:        flusher,
			rdb:            rdb,
			batchSize:      200,
			flushInterval:  time.Hour,
			retryBaseDelay: time.Millisecond,
		}

		ctx, cancel := context.WithCancel(context.Background())
		tracker := &mockAckTracker{}
		deliveries := make(chan mq.StreamDelivery, 3)
		deliveries <- makeStreamDelivery(1, mustEventBytes(t, 99, 1, model.InteractionActionLike, 1), tracker)
		deliveries <- makeStreamDelivery(2, mustEventBytes(t, 99, 2, model.InteractionActionLike, 1), tracker)
		deliveries <- makeStreamDelivery(3, mustEventBytes(t, 99, 3, model.InteractionActionDownload, 1), tracker)

		// 模拟收到 SIGTERM 后 ctx 先取消、Broker 排空后关闭 deliveries。
		cancel()
		close(deliveries)

		if err := consumer.runSession(ctx, deliveries); err != nil {
			t.Fatalf("graceful shutdown final flush failed: %v", err)
		}
		if !flusher.lastCtxAlive {
			t.Fatalf("final flush context was already canceled, must use context.WithoutCancel")
		}
		calls := flusher.snapshotCalls()
		if len(calls) != 1 || calls[0][0] != (model.StatsDelta{WallpaperID: 99, Like: 2, Download: 1}) {
			t.Fatalf("final flush delta mismatch: %+v", calls)
		}
		if len(tracker.acked) != 1 || !tracker.acked[0] || tracker.ackedTags[0] != 3 {
			t.Fatalf("expected final batch Ack(true) on tag 3, got acked=%v tags=%v", tracker.acked, tracker.ackedTags)
		}
		if mr.Exists(cache.WallpaperStatsKey(99)) {
			t.Fatalf("expected L2 key unlinked on final flush")
		}
	})

	t.Run("unexpected broker disconnect skips db flush to avoid duplicate increment on redelivery", func(t *testing.T) {
		flusher := &stubStatsFlusher{}
		consumer := &InteractionAggConsumer{
			flusher:        flusher,
			batchSize:      200,
			flushInterval:  time.Hour,
			retryBaseDelay: time.Millisecond,
		}

		tracker := &mockAckTracker{}
		deliveries := make(chan mq.StreamDelivery, 2)
		deliveries <- makeStreamDelivery(1, mustEventBytes(t, 99, 1, model.InteractionActionLike, 1), tracker)
		close(deliveries) // ctx 未取消，模拟 TCP 意外闪断

		if err := consumer.runSession(context.Background(), deliveries); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(flusher.snapshotCalls()) != 0 || len(tracker.acked) != 0 {
			t.Fatalf("unexpected disconnect must not flush or ack, calls=%v acked=%v", flusher.snapshotCalls(), tracker.acked)
		}
	})
}
