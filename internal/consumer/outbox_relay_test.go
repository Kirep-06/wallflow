package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/mq"
)

type memOutboxStore struct {
	mu         sync.Mutex
	events     []*model.OutboxEvent
	claimCalls int
}

func newMemOutboxStore(events ...*model.OutboxEvent) *memOutboxStore {
	cloned := make([]*model.OutboxEvent, len(events))
	for i, ev := range events {
		cp := *ev
		cloned[i] = &cp
	}
	return &memOutboxStore{events: cloned}
}

func (s *memOutboxStore) ClaimBatch(_ context.Context, limit int, lease time.Duration, now time.Time) ([]*model.OutboxEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++
	var claimed []*model.OutboxEvent
	for _, ev := range s.events {
		if ev.Status != model.OutboxStatusPending {
			continue
		}
		if ev.NextRetryAt.After(now) {
			continue
		}
		ev.NextRetryAt = now.Add(lease)
		ev.UpdatedAt = now
		cp := *ev
		claimed = append(claimed, &cp)
		if len(claimed) >= limit {
			break
		}
	}
	return claimed, nil
}

func (s *memOutboxStore) MarkSent(_ context.Context, ids []uint64, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idSet := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	for _, ev := range s.events {
		if _, ok := idSet[ev.ID]; ok && ev.Status == model.OutboxStatusPending {
			ev.Status = model.OutboxStatusSent
			ev.UpdatedAt = now
		}
	}
	return nil
}

func (s *memOutboxStore) MarkFailed(_ context.Context, id uint64, retryCount uint16, nextRetryAt time.Time, dead bool, lastErr string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range s.events {
		if ev.ID == id && ev.Status == model.OutboxStatusPending {
			if dead {
				ev.Status = model.OutboxStatusDead
			} else {
				ev.Status = model.OutboxStatusPending
			}
			ev.RetryCount = retryCount
			ev.NextRetryAt = nextRetryAt
			ev.LastError = lastErr
			ev.UpdatedAt = now
			break
		}
	}
	return nil
}

func (s *memOutboxStore) ReleaseLease(_ context.Context, ids []uint64, nextRetryAt, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idSet := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	for _, ev := range s.events {
		if _, ok := idSet[ev.ID]; ok && ev.Status == model.OutboxStatusPending {
			ev.NextRetryAt = nextRetryAt
			ev.UpdatedAt = now
		}
	}
	return nil
}

func (s *memOutboxStore) snapshot(id uint64) model.OutboxEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range s.events {
		if ev.ID == id {
			return *ev
		}
	}
	panic(fmt.Sprintf("event %d not found", id))
}

type stubPublisher struct {
	mu        sync.Mutex
	publishFn func(ctx context.Context, msg mq.Message) error
	delivered []string
	attempts  int
}

func (p *stubPublisher) Publish(ctx context.Context, msg mq.Message) error {
	p.mu.Lock()
	p.attempts++
	fn := p.publishFn
	p.mu.Unlock()

	err := fn(ctx, msg)
	if err == nil {
		p.mu.Lock()
		p.delivered = append(p.delivered, msg.MessageID)
		p.mu.Unlock()
	}
	return err
}

func makePendingEvent(id uint64, createdAt time.Time) *model.OutboxEvent {
	return &model.OutboxEvent{
		ID:           id,
		EventID:      fmt.Sprintf("post.created:%d", id),
		ExchangeName: "wallpaper.events",
		RoutingKey:   model.EventTypePostCreated,
		Payload:      fmt.Sprintf(`{"post_id":%d}`, id),
		Status:       model.OutboxStatusPending,
		RetryCount:   0,
		NextRetryAt:  createdAt,
		CreatedAt:    createdAt,
		UpdatedAt:    createdAt,
	}
}

// TestOutboxRelay_ConnectionOutageBeyondMaxRetriesThenRecovers 回归问题 2：
// MQ 持续离线超过原最大重试次数（MaxRetries=3，持续离线失败 12 轮）期间：
// - 事件始终保持 Pending(status=0)、retry_count 不递增、绝不进入永久死信(status=2)；
// - 连接故障退避严格有界（2s -> 4s -> 8s -> 16s -> 30s 封顶），且冷却期内不空转刷库；
// - MQ 恢复后自动成功投递全部事件。
func TestOutboxRelay_ConnectionOutageBeyondMaxRetriesThenRecovers(t *testing.T) {
	ctx := context.Background()
	curTime := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

	store := newMemOutboxStore(
		makePendingEvent(1, curTime),
		makePendingEvent(2, curTime),
		makePendingEvent(3, curTime),
	)

	connErrors := []error{
		mq.ErrConnectionUnavailable,
		amqp.ErrClosed,
		fmt.Errorf("%w: %w", mq.ErrConnectionUnavailable, context.DeadlineExceeded),
	}
	var online bool
	attemptIdx := 0
	pub := &stubPublisher{
		publishFn: func(_ context.Context, _ mq.Message) error {
			if !online {
				err := connErrors[attemptIdx%len(connErrors)]
				attemptIdx++
				return err
			}
			return nil
		},
	}

	relay := NewOutboxRelay(store, pub, config.OutboxConfig{
		PollIntervalMs:   1000,
		BatchSize:        50,
		LeaseSeconds:     30,
		PublishTimeoutMs: 5000,
		MaxRetries:       3, // 故意设置很小，验证持续离线 12 轮不会耗尽预算
		RetentionDays:    7,
	})
	relay.now = func() time.Time { return curTime }

	wantBackoffs := []time.Duration{
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
		30 * time.Second,
		30 * time.Second,
		30 * time.Second,
		30 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}

	for round, wantBackoff := range wantBackoffs {
		relay.drain(ctx)

		// 验证冷却期内再次 drain 不会空转查库。
		claimsAfterRound := store.claimCalls
		curTime = curTime.Add(wantBackoff - time.Millisecond)
		relay.drain(ctx)
		if store.claimCalls != claimsAfterRound {
			t.Fatalf("round %d: drain during backoff should not query DB (claims %d -> %d)", round+1, claimsAfterRound, store.claimCalls)
		}

		// 验证全部事件均为 Pending、RetryCount=0，且 NextRetryAt 精确为退避截止点。
		for _, id := range []uint64{1, 2, 3} {
			ev := store.snapshot(id)
			if ev.Status != model.OutboxStatusPending {
				t.Fatalf("round %d: event %d unexpectedly changed status to %d during outage", round+1, id, ev.Status)
			}
			if ev.RetryCount != 0 {
				t.Fatalf("round %d: event %d retry_count incremented to %d during outage", round+1, id, ev.RetryCount)
			}
		}
		ev1 := store.snapshot(1)
		if ev1.LastError == "" {
			t.Fatalf("round %d: expected last_error recorded on triggering event for diagnostics", round+1)
		}
		wantNext := ev1.UpdatedAt.Add(wantBackoff)
		if !ev1.NextRetryAt.Equal(wantNext) {
			t.Fatalf("round %d: backoff mismatch: got %v, want %v", round+1, ev1.NextRetryAt.Sub(ev1.UpdatedAt), wantBackoff)
		}

		// 推进过退避边界进入下一轮。
		curTime = curTime.Add(time.Millisecond)
	}

	// 确认已持续离线超过原重试上限（12 轮 >> MaxRetries=3）。
	if pub.attempts != len(wantBackoffs) {
		t.Fatalf("expected %d outage publish probes (1 per batch abort), got %d", len(wantBackoffs), pub.attempts)
	}

	// 模拟 MQ 恢复：下一轮自动成功投递全部 3 条事件并复位退避状态。
	online = true
	relay.drain(ctx)

	for _, id := range []uint64{1, 2, 3} {
		ev := store.snapshot(id)
		if ev.Status != model.OutboxStatusSent {
			t.Fatalf("after recovery: event %d status=%d, want Sent(%d)", id, ev.Status, model.OutboxStatusSent)
		}
		if ev.RetryCount != 0 {
			t.Fatalf("after recovery: event %d retry_count=%d, want 0", id, ev.RetryCount)
		}
	}
	wantDelivered := []string{"post.created:1", "post.created:2", "post.created:3"}
	if !slices.Equal(pub.delivered, wantDelivered) {
		t.Fatalf("delivered events mismatch: got %v, want %v", pub.delivered, wantDelivered)
	}
	if relay.connFailures != 0 || !relay.connBackoffUntil.IsZero() {
		t.Fatalf("expected connection backoff reset after recovery, got failures=%d until=%v", relay.connFailures, relay.connBackoffUntil)
	}
}

// TestOutboxRelay_NonConnectionErrorConsumesBudgetAndDeadLetters 验证：
// 1) 非连接错误（404 拓扑错误 / Broker Nack 等）会递增单事件 retry_count，达上限进入 status=2 死信，不阻断同批正常事件；
// 2) 元数据损坏的不可恢复事件直接转入 status=2 死信。
func TestOutboxRelay_NonConnectionErrorConsumesBudgetAndDeadLetters(t *testing.T) {
	ctx := context.Background()
	curTime := time.Date(2026, 11, 21, 13, 0, 0, 0, time.UTC)

	badMeta := makePendingEvent(1, curTime)
	badMeta.ExchangeName = "" // 不可恢复的坏元数据

	nackEv := makePendingEvent(2, curTime)
	notFoundEv := makePendingEvent(3, curTime)
	goodEv := makePendingEvent(4, curTime)

	store := newMemOutboxStore(badMeta, nackEv, notFoundEv, goodEv)
	pub := &stubPublisher{
		publishFn: func(_ context.Context, msg mq.Message) error {
			switch msg.MessageID {
			case "post.created:2":
				return mq.ErrPublishNacked
			case "post.created:3":
				return &amqp.Error{Code: amqp.NotFound, Reason: "no exchange", Server: true}
			case "post.created:4":
				return nil
			default:
				return errors.New("unexpected message")
			}
		},
	}

	relay := NewOutboxRelay(store, pub, config.OutboxConfig{
		PollIntervalMs:   1000,
		BatchSize:        50,
		LeaseSeconds:     30,
		PublishTimeoutMs: 5000,
		MaxRetries:       3,
		RetentionDays:    7,
	})
	relay.now = func() time.Time { return curTime }

	// 第 1 轮：badMeta 直接死信，goodEv 直接成功，nackEv 与 notFoundEv 记 retry_count=1 且保持 Pending。
	relay.drain(ctx)

	if ev := store.snapshot(1); ev.Status != model.OutboxStatusDead {
		t.Fatalf("bad metadata event should move directly to dead, got status=%d", ev.Status)
	}
	if ev := store.snapshot(4); ev.Status != model.OutboxStatusSent {
		t.Fatalf("healthy event in same batch should be sent, got status=%d", ev.Status)
	}
	for _, id := range []uint64{2, 3} {
		ev := store.snapshot(id)
		if ev.Status != model.OutboxStatusPending || ev.RetryCount != 1 {
			t.Fatalf("event %d after 1st non-conn error: status=%d retry=%d", id, ev.Status, ev.RetryCount)
		}
	}
	if relay.connFailures != 0 {
		t.Fatalf("non-connection errors must not trigger connection backoff, got connFailures=%d", relay.connFailures)
	}

	// 第 2 轮（推进 1s）：retry_count -> 2。
	curTime = curTime.Add(1 * time.Second)
	relay.drain(ctx)
	for _, id := range []uint64{2, 3} {
		ev := store.snapshot(id)
		if ev.Status != model.OutboxStatusPending || ev.RetryCount != 2 {
			t.Fatalf("event %d after 2nd non-conn error: status=%d retry=%d", id, ev.Status, ev.RetryCount)
		}
	}

	// 第 3 轮（推进 2s）：达到 MaxRetries=3，置为 status=2 死信。
	curTime = curTime.Add(2 * time.Second)
	relay.drain(ctx)
	for _, id := range []uint64{2, 3} {
		ev := store.snapshot(id)
		if ev.Status != model.OutboxStatusDead || ev.RetryCount != 3 || ev.LastError == "" {
			t.Fatalf("event %d after 3rd non-conn error should be dead: %+v", id, ev)
		}
	}
}

// TestOutboxRelay_PartialBatchSuccessThenConnectionDrop 验证：
// 同批前一条成功、下一条遇到连接断开时，成功的正常 MarkSent，触发断连及剩余未试事件均保 Pending、不增 retry_count，恢复后全部成功。
func TestOutboxRelay_PartialBatchSuccessThenConnectionDrop(t *testing.T) {
	ctx := context.Background()
	curTime := time.Date(2026, 11, 21, 14, 0, 0, 0, time.UTC)

	store := newMemOutboxStore(
		makePendingEvent(1, curTime),
		makePendingEvent(2, curTime),
		makePendingEvent(3, curTime),
		makePendingEvent(4, curTime),
	)

	dropSecond := true
	pub := &stubPublisher{
		publishFn: func(_ context.Context, msg mq.Message) error {
			if dropSecond && msg.MessageID == "post.created:2" {
				return mq.ErrConnectionUnavailable
			}
			return nil
		},
	}

	relay := NewOutboxRelay(store, pub, config.OutboxConfig{
		PollIntervalMs:   1000,
		BatchSize:        50,
		LeaseSeconds:     30,
		PublishTimeoutMs: 5000,
		MaxRetries:       3,
		RetentionDays:    7,
	})
	relay.now = func() time.Time { return curTime }

	relay.drain(ctx)

	if pub.attempts != 2 {
		t.Fatalf("expected relay to stop batch immediately after 2nd event connection error, attempts=%d", pub.attempts)
	}
	if ev1 := store.snapshot(1); ev1.Status != model.OutboxStatusSent {
		t.Fatalf("event 1 should be Sent, got %+v", ev1)
	}
	wantDefer := curTime.Add(outboxConnBackoffBase)
	for _, id := range []uint64{2, 3, 4} {
		ev := store.snapshot(id)
		if ev.Status != model.OutboxStatusPending || ev.RetryCount != 0 || !ev.NextRetryAt.Equal(wantDefer) {
			t.Fatalf("event %d should remain Pending with retry=0 deferred to %v, got %+v", id, wantDefer, ev)
		}
	}

	// 恢复连接并推进过退避时间，剩余 3 条全部投递成功。
	dropSecond = false
	curTime = wantDefer
	relay.drain(ctx)

	for _, id := range []uint64{1, 2, 3, 4} {
		if ev := store.snapshot(id); ev.Status != model.OutboxStatusSent {
			t.Fatalf("event %d should be Sent after recovery, got %+v", id, ev)
		}
	}
}
