package cache

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	gocache "github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const (
	// NilMarker 为防缓存穿透写入 L2 Redis 的空值哨兵标记。
	NilMarker = "\x00nil"

	defaultL1TTL         = 30 * time.Second
	defaultL2TTL         = 1800 * time.Second
	defaultNullL2TTL     = 60 * time.Second
	defaultJitterRatio   = 0.1
	defaultLoaderTimeout = 5 * time.Second
)

// ErrNotFound 表示回源或命中空值缓存时目标记录不存在。
var ErrNotFound = errors.New("cache: not found")

// Options 配置 MultiLevel 三级缓存的 TTL 与抖动参数。
type Options struct {
	L1TTL         time.Duration
	L2TTL         time.Duration
	NullL2TTL     time.Duration
	JitterRatio   float64
	DisableJitter bool
	LoaderTimeout time.Duration
	Logger        *slog.Logger
}

type l1Entry[T any] struct {
	value T
	isNil bool
}

type sfResult[T any] struct {
	value    T
	notFound bool
}

// MultiLevel 实现通用的 L1(gocache) -> L2(Redis) -> L3(Loader) 三级缓存。
// 内置 singleflight 防击穿、空值标记防穿透、L2 ±10% TTL 抖动防雪崩，
// 以及 Redis 故障时透明降级回源能力。
type MultiLevel[T any] struct {
	l1            *gocache.Cache
	l2            redis.Cmdable
	l1TTL         time.Duration
	l2TTL         time.Duration
	nullL2TTL     time.Duration
	nullL1TTL     time.Duration
	jitterRatio   float64
	loaderTimeout time.Duration
	logger        *slog.Logger
	sf            singleflight.Group
}

// NewMultiLevel 构造指定类型的通用三级缓存组件。
func NewMultiLevel[T any](l1 *gocache.Cache, l2 redis.Cmdable, opts Options) *MultiLevel[T] {
	l1TTL := opts.L1TTL
	if l1TTL <= 0 {
		l1TTL = defaultL1TTL
	}
	l2TTL := opts.L2TTL
	if l2TTL <= 0 {
		l2TTL = defaultL2TTL
	}
	nullL2TTL := opts.NullL2TTL
	if nullL2TTL <= 0 {
		nullL2TTL = defaultNullL2TTL
	}
	nullL1TTL := min(l1TTL, nullL2TTL)

	jitter := opts.JitterRatio
	if opts.DisableJitter {
		jitter = 0
	} else if jitter <= 0 {
		jitter = defaultJitterRatio
	}
	if jitter > 0.5 {
		jitter = 0.5
	}

	loaderTimeout := opts.LoaderTimeout
	if loaderTimeout <= 0 {
		loaderTimeout = defaultLoaderTimeout
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &MultiLevel[T]{
		l1:            l1,
		l2:            l2,
		l1TTL:         l1TTL,
		l2TTL:         l2TTL,
		nullL2TTL:     nullL2TTL,
		nullL1TTL:     nullL1TTL,
		jitterRatio:   jitter,
		loaderTimeout: loaderTimeout,
		logger:        logger,
	}
}

// Get 按 L1 -> L2 -> singleflight(L3 Loader) 顺序读取缓存，未命中时自动回填。
func (m *MultiLevel[T]) Get(ctx context.Context, key string, loader func(context.Context) (T, error)) (T, error) {
	var zero T

	// 1. 查询 L1 进程内缓存。
	if val, hit, notFound := m.getL1(key); hit {
		if notFound {
			return zero, ErrNotFound
		}
		return val, nil
	}

	if err := ctx.Err(); err != nil {
		return zero, err
	}

	// 2. 查询 L2 Redis；Redis 异常时仅告警并透明降级回源。
	if val, hit, notFound := m.getL2(ctx, key); hit {
		if notFound {
			m.setL1Nil(key)
			return zero, ErrNotFound
		}
		m.setL1Value(key, val)
		return val, nil
	}

	// 3. 通过 singleflight 合并同 key 并发回源请求。
	ch := m.sf.DoChan(key, func() (any, error) {
		// 进入单飞后二次检查 L1，避免刚被前序回源填充后重复查询。
		if val, hit, notFound := m.getL1(key); hit {
			return sfResult[T]{value: val, notFound: notFound}, nil
		}

		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.loaderTimeout)
		defer cancel()

		val, err := loader(loadCtx)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				m.setL2Nil(loadCtx, key)
				m.setL1Nil(key)
				return sfResult[T]{notFound: true}, nil
			}
			return sfResult[T]{}, err
		}

		m.setL2Value(loadCtx, key, val)
		m.setL1Value(key, val)
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
		m.l1.Delete(key)
	}
	m.sf.Forget(key)
	if m.l2 != nil {
		if err := m.l2.Del(ctx, key).Err(); err != nil {
			m.logger.WarnContext(ctx, "cache l2 delete failed",
				slog.String("key", key),
				slog.Any("err", err),
			)
			return err
		}
	}
	return nil
}

func (m *MultiLevel[T]) getL1(key string) (val T, hit bool, notFound bool) {
	var zero T
	if m.l1 == nil {
		return zero, false, false
	}
	raw, ok := m.l1.Get(key)
	if !ok {
		return zero, false, false
	}
	entry, ok := raw.(l1Entry[T])
	if !ok {
		return zero, false, false
	}
	if entry.isNil {
		return zero, true, true
	}
	return entry.value, true, false
}

func (m *MultiLevel[T]) setL1Value(key string, val T) {
	if m.l1 == nil {
		return
	}
	m.l1.Set(key, l1Entry[T]{value: val}, m.l1TTL)
}

func (m *MultiLevel[T]) setL1Nil(key string) {
	if m.l1 == nil {
		return
	}
	m.l1.Set(key, l1Entry[T]{isNil: true}, m.nullL1TTL)
}

func (m *MultiLevel[T]) getL2(ctx context.Context, key string) (val T, hit bool, notFound bool) {
	var zero T
	if m.l2 == nil {
		return zero, false, false
	}
	raw, err := m.l2.Get(ctx, key).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			m.logger.WarnContext(ctx, "cache l2 get degraded to loader",
				slog.String("key", key),
				slog.Any("err", err),
			)
		}
		return zero, false, false
	}
	if raw == NilMarker {
		return zero, true, true
	}
	var decoded T
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		m.logger.WarnContext(ctx, "cache l2 unmarshal failed, reloading from loader",
			slog.String("key", key),
			slog.Any("err", err),
		)
		return zero, false, false
	}
	return decoded, true, false
}

func (m *MultiLevel[T]) setL2Value(ctx context.Context, key string, val T) {
	if m.l2 == nil {
		return
	}
	encoded, err := json.Marshal(val)
	if err != nil {
		m.logger.WarnContext(ctx, "cache l2 marshal failed",
			slog.String("key", key),
			slog.Any("err", err),
		)
		return
	}
	ttl := m.applyJitter(m.l2TTL)
	if err := m.l2.Set(ctx, key, encoded, ttl).Err(); err != nil {
		m.logger.WarnContext(ctx, "cache l2 set failed",
			slog.String("key", key),
			slog.Any("err", err),
		)
	}
}

func (m *MultiLevel[T]) setL2Nil(ctx context.Context, key string) {
	if m.l2 == nil {
		return
	}
	ttl := m.applyJitter(m.nullL2TTL)
	if err := m.l2.Set(ctx, key, NilMarker, ttl).Err(); err != nil {
		m.logger.WarnContext(ctx, "cache l2 set nil marker failed",
			slog.String("key", key),
			slog.Any("err", err),
		)
	}
}

func (m *MultiLevel[T]) applyJitter(base time.Duration) time.Duration {
	if base <= 0 || m.jitterRatio <= 0 {
		return base
	}
	// 在 [1 - ratio, 1 + ratio] 区间随机抖动，避免大批量缓存同时失效引发雪崩。
	factor := 1 + (rand.Float64()*2-1)*m.jitterRatio
	jittered := time.Duration(float64(base) * factor)
	if jittered < time.Millisecond {
		return time.Millisecond
	}
	return jittered
}
