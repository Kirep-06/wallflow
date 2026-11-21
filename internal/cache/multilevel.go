// Package cache 实现 L1(gocache) → L2(Redis) → L3(加载函数，通常是 MySQL) 的三级缓存编排。
//
// 防护策略（PROJECT_GUIDE 6.3）：
//   - 击穿：同一批 ID 回源时以 singleflight 合并；
//   - 穿透：L3 查不到的 ID 写入空值标记；
//   - 雪崩：L2 TTL 加 ±10% 随机抖动。
package cache

import (
	"bytes"
	"context"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// NullMarker 表示“确认不存在”，用于防穿透。
var NullMarker = []byte("\x00nil")

// Loader 从 L3 批量加载数据；返回的 map 只包含存在的 ID。
type Loader func(ctx context.Context, ids []uint64) (map[uint64][]byte, error)

// TTLFunc 决定某个 ID 回填 L2 时的基础 TTL（例如区分冷/热数据）。
type TTLFunc func(id uint64) time.Duration

type Options struct {
	Prefix   string        // key 前缀，如 "post:detail:"
	LocalTTL time.Duration // L1 TTL
	NullTTL  time.Duration // 空值标记 TTL（L1 取 min(LocalTTL, NullTTL)）
}

type MultiLevel struct {
	local *gocache.Cache
	rdb   redis.UniversalClient
	opt   Options
	sf    singleflight.Group
}

func NewMultiLevel(local *gocache.Cache, rdb redis.UniversalClient, opt Options) *MultiLevel {
	return &MultiLevel{local: local, rdb: rdb, opt: opt}
}

func (m *MultiLevel) key(id uint64) string { return m.opt.Prefix + strconv.FormatUint(id, 10) }

// GetMany 按 L1 → L2 → L3 顺序批量读取，命中下层后逐层回填。返回值只包含存在的 ID。
// L2 故障时降级直接回源 L3，不向调用方报错；只有 L3 失败才返回 error。
func (m *MultiLevel) GetMany(ctx context.Context, ids []uint64, load Loader, ttl TTLFunc) (map[uint64][]byte, error) {
	result := make(map[uint64][]byte, len(ids))
	missing := make([]uint64, 0, len(ids))

	// L1
	for _, id := range ids {
		if _, dup := result[id]; dup || slices.Contains(missing, id) {
			continue
		}
		v, err := m.local.Get([]byte(m.key(id)))
		if err != nil {
			missing = append(missing, id)
			continue
		}
		if !bytes.Equal(v, NullMarker) {
			result[id] = v
		}
	}
	if len(missing) == 0 {
		return result, nil
	}

	// L2
	missing = m.fromRedis(ctx, missing, result)
	if len(missing) == 0 {
		return result, nil
	}

	// L3：相同的 ID 集合合并为一次回源
	slices.Sort(missing)
	loaded, err, _ := m.sf.Do(sfKey(missing), func() (any, error) {
		return m.loadAndFill(ctx, missing, load, ttl)
	})
	if err != nil {
		return nil, err
	}
	for id, v := range loaded.(map[uint64][]byte) {
		result[id] = v
	}
	return result, nil
}

func (m *MultiLevel) fromRedis(ctx context.Context, ids []uint64, result map[uint64][]byte) []uint64 {
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = m.key(id)
	}
	vals, err := m.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		slog.WarnContext(ctx, "cache l2 mget failed, fallback to l3", "err", err)
		return ids
	}
	var missing []uint64
	for i, raw := range vals {
		s, ok := raw.(string)
		if !ok {
			missing = append(missing, ids[i])
			continue
		}
		v := []byte(s)
		m.setLocal(keys[i], v)
		if !bytes.Equal(v, NullMarker) {
			result[ids[i]] = v
		}
	}
	return missing
}

func (m *MultiLevel) loadAndFill(ctx context.Context, ids []uint64, load Loader, ttl TTLFunc) (map[uint64][]byte, error) {
	loaded, err := load(ctx, ids)
	if err != nil {
		return nil, err
	}
	pipe := m.rdb.Pipeline()
	for _, id := range ids {
		k := m.key(id)
		if v, ok := loaded[id]; ok {
			pipe.Set(ctx, k, v, Jitter(ttl(id)))
			m.setLocal(k, v)
		} else {
			pipe.Set(ctx, k, NullMarker, Jitter(m.opt.NullTTL))
			m.setLocal(k, NullMarker)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		slog.WarnContext(ctx, "cache l2 backfill failed", "err", err)
	}
	return loaded, nil
}

func (m *MultiLevel) setLocal(key string, v []byte) {
	ttl := m.opt.LocalTTL
	if bytes.Equal(v, NullMarker) && m.opt.NullTTL < ttl {
		ttl = m.opt.NullTTL
	}
	sec := int(ttl / time.Second)
	if sec <= 0 {
		return // TTL 不足 1 秒时不写 L1，避免 gocache 将 0 视为永不过期
	}
	_ = m.local.Set([]byte(key), v, sec)
}

// Delete 删除 L2 与 L1 中的缓存（写路径：先写 MySQL，再删缓存）。
func (m *MultiLevel) Delete(ctx context.Context, ids ...uint64) error {
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = m.key(id)
		m.local.Del([]byte(keys[i]))
	}
	return m.rdb.Del(ctx, keys...).Err()
}

// Jitter 返回 d ±10% 的随机 TTL，防止缓存雪崩。
func Jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	delta := int64(d) / 10
	if delta == 0 {
		return d
	}
	return d + time.Duration(rand.Int64N(2*delta+1)-delta)
}

func sfKey(sortedIDs []uint64) string {
	var b strings.Builder
	for i, id := range sortedIDs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(id, 10))
	}
	return b.String()
}
