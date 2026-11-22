package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const (
	// NilMarker 为防缓存穿透写入 L1/L2 的空值哨兵标记。
	NilMarker = "\x00nil"

	defaultL1TTL  = 30 * time.Second
	defaultL2TTL  = 1800 * time.Second
	nullL2TTL     = 60 * time.Second
	jitterRatio   = 0.1
	loaderTimeout = 5 * time.Second
)

var (
	// ErrNotFound 表示回源或命中空值缓存时目标记录不存在。
	ErrNotFound    = errors.New("cache: not found")
	nilMarkerBytes = []byte(NilMarker)
)

type sfResult[T any] struct {
	value    T
	notFound bool
}

// MultiLevel 实现通用的 L1(Kirep-06/gocache) -> L2(Redis) -> L3(Loader) 三级缓存。
// 内置 singleflight 防击穿、空值标记防穿透、L2 ±10% TTL 抖动防雪崩，
// 以及 Redis 故障时透明降级回源能力。
type MultiLevel[T any] struct {
	l1           *gocache.Cache
	l2           *redis.Client
	l1TTLSec     int
	nullL1TTLSec int
	l2TTL        time.Duration
	sf           singleflight.Group
}

// NewMultiLevel 构造指定类型的通用三级缓存组件。
// l1TTL / l2TTL 传入非正值时自动使用默认值（30s / 30m）；
// 空值缓存 TTL（60s）、L2 抖动（±10%）与回源超时（5s）使用统一内置策略。
func NewMultiLevel[T any](l1 *gocache.Cache, l2 *redis.Client, l1TTL, l2TTL time.Duration) *MultiLevel[T] {
	if l1TTL <= 0 {
		l1TTL = defaultL1TTL
	}
	if l2TTL <= 0 {
		l2TTL = defaultL2TTL
	}

	return &MultiLevel[T]{
		l1:           l1,
		l2:           l2,
		l1TTLSec:     ttlToSeconds(l1TTL),
		nullL1TTLSec: ttlToSeconds(min(l1TTL, nullL2TTL)),
		l2TTL:        l2TTL,
	}
}

// Get 按 L1 -> L2 -> singleflight(L3 Loader) 顺序读取缓存，未命中时自动回填。
func (m *MultiLevel[T]) Get(ctx context.Context, key string, loader func(context.Context) (T, error)) (T, error) {
	var zero T

	// 1. 查询 L1 进程内缓存（Kirep-06/gocache）。
	if val, hit, notFound := m.getL1(ctx, key); hit {
		if notFound {
			return zero, ErrNotFound
		}
		return val, nil
	}

	if err := ctx.Err(); err != nil {
		return zero, err
	}

	// 2. 查询 L2 Redis；Redis 异常时仅告警并透明降级回源。
	if val, raw, hit, notFound := m.getL2(ctx, key); hit {
		if notFound {
			m.setL1Bytes(ctx, key, nilMarkerBytes, m.nullL1TTLSec)
			return zero, ErrNotFound
		}
		m.setL1Bytes(ctx, key, raw, m.l1TTLSec)
		return val, nil
	}

	// 3. 通过 singleflight 合并同 key 并发回源请求。
	ch := m.sf.DoChan(key, func() (any, error) {
		if val, hit, notFound := m.getL1(ctx, key); hit {
			return sfResult[T]{value: val, notFound: notFound}, nil
		}

		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), loaderTimeout)
		defer cancel()

		val, err := loader(loadCtx)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				m.setL2Nil(loadCtx, key)
				m.setL1Bytes(loadCtx, key, nilMarkerBytes, m.nullL1TTLSec)
				return sfResult[T]{notFound: true}, nil
			}
			return sfResult[T]{}, err
		}

		encoded, err := json.Marshal(val)
		if err != nil {
			slog.WarnContext(loadCtx, "cache marshal value failed",
				slog.String("key", key),
				slog.Any("err", err),
			)
			return sfResult[T]{value: val}, nil
		}

		m.setL2Bytes(loadCtx, key, encoded)
		m.setL1Bytes(loadCtx, key, encoded, m.l1TTLSec)
		return sfResult[T]{value: val}, nil
	})

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return zero, res.Err
		}
		loaded, ok := res.Val.(sfResult[T])
		if !ok {
			return zero, errors.New("cache: unexpected singleflight result type")
		}
		if loaded.notFound {
			return zero, ErrNotFound
		}
		return loaded.value, nil
	}
}

// Delete 执行写后失效：先删 L1 并遗忘在途单飞，再删 L2 Redis。
func (m *MultiLevel[T]) Delete(ctx context.Context, key string) error {
	if m.l1 != nil {
		m.l1.Del([]byte(key))
	}
	m.sf.Forget(key)
	if m.l2 != nil {
		if err := m.l2.Del(ctx, key).Err(); err != nil {
			slog.WarnContext(ctx, "cache l2 delete failed",
				slog.String("key", key),
				slog.Any("err", err),
			)
			return err
		}
	}
	return nil
}

func (m *MultiLevel[T]) getL1(ctx context.Context, key string) (val T, hit bool, notFound bool) {
	var zero T
	if m.l1 == nil {
		return zero, false, false
	}
	raw, err := m.l1.Get([]byte(key))
	if err != nil {
		if !errors.Is(err, gocache.ErrNotFound) {
			slog.WarnContext(ctx, "cache l1 get failed",
				slog.String("key", key),
				slog.Any("err", err),
			)
		}
		return zero, false, false
	}
	if bytes.Equal(raw, nilMarkerBytes) {
		return zero, true, true
	}
	var decoded T
	if err := json.Unmarshal(raw, &decoded); err != nil {
		slog.WarnContext(ctx, "cache l1 unmarshal failed",
			slog.String("key", key),
			slog.Any("err", err),
		)
		return zero, false, false
	}
	return decoded, true, false
}

func (m *MultiLevel[T]) setL1Bytes(ctx context.Context, key string, data []byte, ttlSec int) {
	if m.l1 == nil {
		return
	}
	if err := m.l1.Set([]byte(key), data, ttlSec); err != nil {
		slog.WarnContext(ctx, "cache l1 set failed",
			slog.String("key", key),
			slog.Any("err", err),
		)
	}
}

func (m *MultiLevel[T]) getL2(ctx context.Context, key string) (val T, raw []byte, hit bool, notFound bool) {
	var zero T
	if m.l2 == nil {
		return zero, nil, false, false
	}
	data, err := m.l2.Get(ctx, key).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			slog.WarnContext(ctx, "cache l2 get degraded to loader",
				slog.String("key", key),
				slog.Any("err", err),
			)
		}
		return zero, nil, false, false
	}
	if bytes.Equal(data, nilMarkerBytes) {
		return zero, nil, true, true
	}
	var decoded T
	if err := json.Unmarshal(data, &decoded); err != nil {
		slog.WarnContext(ctx, "cache l2 unmarshal failed, reloading from loader",
			slog.String("key", key),
			slog.Any("err", err),
		)
		return zero, nil, false, false
	}
	return decoded, data, true, false
}

func (m *MultiLevel[T]) setL2Bytes(ctx context.Context, key string, encoded []byte) {
	if m.l2 == nil {
		return
	}
	ttl := applyJitter(m.l2TTL)
	if err := m.l2.Set(ctx, key, encoded, ttl).Err(); err != nil {
		slog.WarnContext(ctx, "cache l2 set failed",
			slog.String("key", key),
			slog.Any("err", err),
		)
	}
}

func (m *MultiLevel[T]) setL2Nil(ctx context.Context, key string) {
	if m.l2 == nil {
		return
	}
	ttl := applyJitter(nullL2TTL)
	if err := m.l2.Set(ctx, key, NilMarker, ttl).Err(); err != nil {
		slog.WarnContext(ctx, "cache l2 set nil marker failed",
			slog.String("key", key),
			slog.Any("err", err),
		)
	}
}

func applyJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return base
	}
	factor := 1 + (rand.Float64()*2-1)*jitterRatio
	return max(time.Duration(float64(base)*factor), time.Millisecond)
}

func ttlToSeconds(d time.Duration) int {
	return max(int(d/time.Second), 1)
}
