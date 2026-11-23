package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/mq"
)

type stubOutboxPublisher struct {
	fn    func(msg mq.PublishMessage) error
	calls int
}

func (p *stubOutboxPublisher) Publish(_ context.Context, msg mq.PublishMessage) error {
	p.calls++
	if p.fn != nil {
		return p.fn(msg)
	}
	return nil
}

func TestOutboxRelayDeliverBatch(t *testing.T) {
	fixedNow := time.Date(2026, 11, 23, 8, 0, 0, 0, time.UTC)

	t.Run("all published on confirm success", func(t *testing.T) {
		pub := &stubOutboxPublisher{}
		relay := NewOutboxRelay(nil, pub)
		relay.nowFunc = func() time.Time { return fixedNow }

		outcomes, aborted := relay.deliverBatch(context.Background(), []model.OutboxEvent{
			{ID: 1, EventID: "wp_pub:1", ExchangeName: mq.ExchangeEvents, RoutingKey: mq.RoutingKeyWallpaperPublished, Payload: `{"wallpaper_id":1}`},
			{ID: 2, EventID: "wp_pub:2", ExchangeName: mq.ExchangeEvents, RoutingKey: mq.RoutingKeyWallpaperPublished, Payload: `{"wallpaper_id":2}`},
		})
		if aborted || pub.calls != 2 || len(outcomes) != 2 {
			t.Fatalf("unexpected result: aborted=%v calls=%d outcomes=%+v", aborted, pub.calls, outcomes)
		}
		for _, out := range outcomes {
			if !out.Published || out.Dead {
				t.Fatalf("expected published outcome, got %+v", out)
			}
		}
	})

	t.Run("transient error schedules exponential backoff and tenth failure marks dead", func(t *testing.T) {
		longErr := errors.New(strings.Repeat("错", 600))
		pub := &stubOutboxPublisher{fn: func(mq.PublishMessage) error { return longErr }}
		relay := NewOutboxRelay(nil, pub)
		relay.nowFunc = func() time.Time { return fixedNow }

		outcomes, aborted := relay.deliverBatch(context.Background(), []model.OutboxEvent{
			{ID: 10, EventID: "wp_pub:10", RetryCount: 2},
			{ID: 11, EventID: "wp_pub:11", RetryCount: outboxMaxRetries - 1},
		})
		if aborted || len(outcomes) != 2 {
			t.Fatalf("unexpected result: aborted=%v outcomes=%+v", aborted, outcomes)
		}

		// retry_count 2 -> 3: base backoff = 4s (±20% -> [3.2s, 4.8s])
		first := outcomes[0]
		if first.Published || first.Dead || first.RetryCount != 3 {
			t.Fatalf("unexpected first outcome: %+v", first)
		}
		delay := first.NextRetryAt.Sub(fixedNow)
		if delay < 3*time.Second || delay > 5*time.Second {
			t.Fatalf("unexpected backoff delay: %v", delay)
		}
		if len([]rune(first.LastError)) != outboxMaxErrorRunes {
			t.Fatalf("last_error runes = %d, want %d", len([]rune(first.LastError)), outboxMaxErrorRunes)
		}

		// 第 10 次失败进入 DB 死信态 status=2
		second := outcomes[1]
		if second.Published || !second.Dead || second.RetryCount != outboxMaxRetries {
			t.Fatalf("expected dead outcome on max retries, got %+v", second)
		}
	})

	t.Run("disconnect fast-fails remaining events without burning retries", func(t *testing.T) {
		pub := &stubOutboxPublisher{fn: func(msg mq.PublishMessage) error {
			if msg.MessageID == "wp_pub:1" {
				return nil
			}
			return mq.ErrNotConnected
		}}
		relay := NewOutboxRelay(nil, pub)
		relay.nowFunc = func() time.Time { return fixedNow }

		outcomes, aborted := relay.deliverBatch(context.Background(), []model.OutboxEvent{
			{ID: 1, EventID: "wp_pub:1", RetryCount: 0},
			{ID: 2, EventID: "wp_pub:2", RetryCount: 1},
			{ID: 3, EventID: "wp_pub:3", RetryCount: 4},
		})
		if !aborted || pub.calls != 2 || len(outcomes) != 3 {
			t.Fatalf("expected fast-fail after event 2: aborted=%v calls=%d outcomes=%+v", aborted, pub.calls, outcomes)
		}
		if !outcomes[0].Published {
			t.Fatalf("event 1 should be published: %+v", outcomes[0])
		}
		if outcomes[1].RetryCount != 2 || outcomes[1].Dead {
			t.Fatalf("event 2 should increment retry_count: %+v", outcomes[1])
		}
		if outcomes[2].RetryCount != 4 || outcomes[2].Dead || !outcomes[2].NextRetryAt.Equal(fixedNow.Add(outboxDisconnectDelay)) {
			t.Fatalf("event 3 should keep retry_count=4 and defer short delay: %+v", outcomes[2])
		}
	})
}
