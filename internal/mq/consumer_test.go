package mq

import (
	"context"
	"errors"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

type stubAcker struct {
	acked       bool
	nacked      bool
	nackRequeue bool
}

func (a *stubAcker) Ack(bool) error {
	a.acked = true
	return nil
}

func (a *stubAcker) Nack(_ bool, requeue bool) error {
	a.nacked = true
	a.nackRequeue = requeue
	return nil
}

type stubPublisher struct {
	msgs []PublishMessage
	err  error
}

func (p *stubPublisher) Publish(_ context.Context, msg PublishMessage) error {
	if p.err != nil {
		return p.err
	}
	p.msgs = append(p.msgs, msg)
	return nil
}

func TestParseRetryCount(t *testing.T) {
	cases := []struct {
		name    string
		headers amqp.Table
		want    int
	}{
		{name: "nil headers", headers: nil, want: 0},
		{name: "missing key", headers: amqp.Table{}, want: 0},
		{name: "int32", headers: amqp.Table{HeaderRetryCount: int32(3)}, want: 3},
		{name: "int64", headers: amqp.Table{HeaderRetryCount: int64(5)}, want: 5},
		{name: "uint16", headers: amqp.Table{HeaderRetryCount: uint16(2)}, want: 2},
		{name: "negative clamped", headers: amqp.Table{HeaderRetryCount: int32(-4)}, want: 0},
		{name: "unexpected string", headers: amqp.Table{HeaderRetryCount: "abc"}, want: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseRetryCount(tc.headers); got != tc.want {
				t.Fatalf("parseRetryCount() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestHandleDeliveryRouting(t *testing.T) {
	baseOpts := ConsumerOptions{
		Queue:           QueueFollowingFanout,
		MaxRetries:      5,
		RetryExchange:   ExchangeDLX,
		RetryRoutingKey: RoutingKeyFollowingFanoutRetry,
	}

	t.Run("success acks directly", func(t *testing.T) {
		acker := &stubAcker{}
		pub := &stubPublisher{}
		handleDelivery(context.Background(), Delivery{MessageID: "e1", Body: []byte(`{}`)}, acker, pub, baseOpts, func(context.Context, Delivery) error {
			return nil
		})
		if !acker.acked || acker.nacked || len(pub.msgs) != 0 {
			t.Fatalf("unexpected state: acker=%+v pub=%d", acker, len(pub.msgs))
		}
	})

	t.Run("permanent poison skips retry and nacks to dlq", func(t *testing.T) {
		acker := &stubAcker{}
		pub := &stubPublisher{}
		handleDelivery(context.Background(), Delivery{MessageID: "poison", Body: []byte(`bad-json`), RetryCount: 0}, acker, pub, baseOpts, func(context.Context, Delivery) error {
			return PermanentError(errors.New("invalid json"))
		})
		if acker.acked || !acker.nacked || acker.nackRequeue || len(pub.msgs) != 0 {
			t.Fatalf("poison should Nack(requeue=false) without retry publish: acker=%+v pub=%d", acker, len(pub.msgs))
		}
	})

	t.Run("transient error publishes to delayed retry queue then acks", func(t *testing.T) {
		acker := &stubAcker{}
		pub := &stubPublisher{}
		longErr := errors.New(strings.Repeat("x", 400))
		handleDelivery(context.Background(), Delivery{
			MessageID:  "e2",
			Body:       []byte(`{"wallpaper_id":1}`),
			Headers:    amqp.Table{"trace": "t1"},
			RetryCount: 2,
		}, acker, pub, baseOpts, func(context.Context, Delivery) error {
			return longErr
		})
		if !acker.acked || acker.nacked {
			t.Fatalf("expected Ack after retry publish, got %+v", acker)
		}
		if len(pub.msgs) != 1 {
			t.Fatalf("expected 1 retry publish, got %d", len(pub.msgs))
		}
		got := pub.msgs[0]
		if got.Exchange != ExchangeDLX || got.RoutingKey != RoutingKeyFollowingFanoutRetry || got.MessageID != "e2" {
			t.Fatalf("unexpected retry publish target: %+v", got)
		}
		if got.Headers["trace"] != "t1" {
			t.Fatalf("original header lost: %+v", got.Headers)
		}
		if rc := parseRetryCount(got.Headers); rc != 3 {
			t.Fatalf("x-retry-count = %d, want 3", rc)
		}
		if lastErr, _ := got.Headers[HeaderLastError].(string); len(lastErr) != maxHeaderErrorLen {
			t.Fatalf("x-last-error len = %d, want %d", len(lastErr), maxHeaderErrorLen)
		}
	})

	t.Run("retry publish failure requeues without ack to prevent message loss", func(t *testing.T) {
		acker := &stubAcker{}
		pub := &stubPublisher{err: ErrNotConnected}
		handleDelivery(context.Background(), Delivery{MessageID: "e3", RetryCount: 1}, acker, pub, baseOpts, func(context.Context, Delivery) error {
			return errors.New("mysql timeout")
		})
		if acker.acked || !acker.nacked || !acker.nackRequeue {
			t.Fatalf("expected Nack(requeue=true) when retry publish fails, got %+v", acker)
		}
	})

	t.Run("retry count reaches max routes to dlq", func(t *testing.T) {
		acker := &stubAcker{}
		pub := &stubPublisher{}
		handleDelivery(context.Background(), Delivery{MessageID: "e4", RetryCount: 5}, acker, pub, baseOpts, func(context.Context, Delivery) error {
			return errors.New("still failing")
		})
		if acker.acked || !acker.nacked || acker.nackRequeue || len(pub.msgs) != 0 {
			t.Fatalf("exhausted retry should Nack(requeue=false) to dlq: acker=%+v pub=%d", acker, len(pub.msgs))
		}
	})
}
