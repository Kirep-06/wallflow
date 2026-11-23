package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// LatestFeedMaxSize 为全站最新流热窗口在 Redis ZSet 中保留的最大壁纸条数。
	LatestFeedMaxSize = 1000

	latestFeedKey      = "feed:latest"
	latestFeedReadyKey = "feed:latest:ready"
	latestFeedTmpKey   = "feed:latest:tmp"
	latestFeedTmpTTL   = 60 * time.Second
)

var (
	// latestFeedReadScript 原子检查热窗口就绪标志并按游标倒序取最多 count 条壁纸 ID。
	// KEYS = [feed:latest, feed:latest:ready]
	// ARGV = [maxScore, count]
	latestFeedReadScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 0 then
	return {0}
end
local ids = redis.call('ZREVRANGEBYSCORE', KEYS[1], ARGV[1], '-inf', 'LIMIT', 0, tonumber(ARGV[2]))
return {1, ids}
`)

	// latestFeedPushScript 仅在热窗口就绪时条件追加新壁纸 ID 并裁剪保持上限。
	// KEYS = [feed:latest, feed:latest:ready]
	// ARGV = [id, maxSize]
	latestFeedPushScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 0 then
	return 0
end
local id = tonumber(ARGV[1])
local maxSize = tonumber(ARGV[2])
redis.call('ZADD', KEYS[1], id, ARGV[1])
local card = redis.call('ZCARD', KEYS[1])
if card > maxSize then
	redis.call('ZREMRANGEBYRANK', KEYS[1], 0, card - maxSize - 1)
end
return 1
`)

	// latestFeedReconcileSwapScript 合并对账窗口内并发新帖，裁剪后原子切换并置永不过期就绪标记。
	// KEYS = [feed:latest:tmp, feed:latest, feed:latest:ready]
	// ARGV = [dbMaxID, maxSize]
	latestFeedReconcileSwapScript = redis.NewScript(`
local maxSize = tonumber(ARGV[2])
if redis.call('EXISTS', KEYS[2]) == 1 then
	local newer = redis.call('ZRANGEBYSCORE', KEYS[2], '(' .. ARGV[1], '+inf')
	for _, member in ipairs(newer) do
		local score = tonumber(member)
		if score ~= nil then
			redis.call('ZADD', KEYS[1], score, member)
		end
	end
end
local card = redis.call('ZCARD', KEYS[1])
if card > maxSize then
	redis.call('ZREMRANGEBYRANK', KEYS[1], 0, card - maxSize - 1)
end
if redis.call('ZCARD', KEYS[1]) > 0 then
	redis.call('RENAME', KEYS[1], KEYS[2])
	redis.call('PERSIST', KEYS[2])
else
	redis.call('DEL', KEYS[1], KEYS[2])
end
redis.call('SET', KEYS[3], '1')
return 1
`)
)

// LatestFeed 封装 L2 Redis ZSet 全站最新流热窗口（feed:latest）与原子对账脚本。
type LatestFeed struct {
	rdb     *redis.Client
	maxSize int
}

// NewLatestFeed 构造最新流热窗口组件（默认容量 LatestFeedMaxSize）。
func NewLatestFeed(rdb *redis.Client) *LatestFeed {
	return &LatestFeed{
		rdb:     rdb,
		maxSize: LatestFeedMaxSize,
	}
}

// ReadPage 原子检查热窗口就绪状态并读取 id < beforeID（beforeID == 0 表示首页）的最多 limit 条壁纸 ID。
func (f *LatestFeed) ReadPage(ctx context.Context, beforeID uint64, limit int) ([]uint64, bool, error) {
	if f == nil || f.rdb == nil || limit <= 0 {
		return nil, false, nil
	}

	maxScore := "+inf"
	if beforeID > 0 {
		maxScore = "(" + strconv.FormatUint(beforeID, 10)
	}

	res, err := latestFeedReadScript.Run(
		ctx,
		f.rdb,
		[]string{latestFeedKey, latestFeedReadyKey},
		maxScore,
		limit,
	).Slice()
	if err != nil {
		return nil, false, fmt.Errorf("latest feed read page: %w", err)
	}
	if len(res) == 0 {
		return nil, false, nil
	}

	readyFlag, ok := res[0].(int64)
	if !ok || readyFlag == 0 {
		return nil, false, nil
	}
	if len(res) < 2 {
		return []uint64{}, true, nil
	}

	rawMembers, ok := res[1].([]any)
	if !ok {
		return nil, false, errors.New("latest feed read page: unexpected members type")
	}

	ids := make([]uint64, 0, len(rawMembers))
	for _, elem := range rawMembers {
		s, ok := elem.(string)
		if !ok {
			continue
		}
		id, err := strconv.ParseUint(s, 10, 64)
		if err != nil || id == 0 {
			continue
		}
		ids = append(ids, id)
	}
	return ids, true, nil
}

// PushIfReady 在热窗口已就绪时将新发布壁纸 ID 条件写入 feed:latest 并裁剪至容量上限。
func (f *LatestFeed) PushIfReady(ctx context.Context, wallpaperID uint64) error {
	if f == nil || f.rdb == nil || wallpaperID == 0 {
		return nil
	}
	if err := latestFeedPushScript.Run(
		ctx,
		f.rdb,
		[]string{latestFeedKey, latestFeedReadyKey},
		strconv.FormatUint(wallpaperID, 10),
		f.maxSize,
	).Err(); err != nil {
		return fmt.Errorf("latest feed push: %w", err)
	}
	return nil
}

// Remove 从热窗口移除已软删壁纸 ID。
func (f *LatestFeed) Remove(ctx context.Context, wallpaperID uint64) error {
	if f == nil || f.rdb == nil || wallpaperID == 0 {
		return nil
	}
	if err := f.rdb.ZRem(ctx, latestFeedKey, strconv.FormatUint(wallpaperID, 10)).Err(); err != nil {
		return fmt.Errorf("latest feed remove: %w", err)
	}
	return nil
}

// Reconcile 使用 MySQL 最新壁纸 ID 快照执行原子对账，保留对账窗口内并发新投稿并置热窗口就绪。
func (f *LatestFeed) Reconcile(ctx context.Context, ids []uint64) error {
	if f == nil || f.rdb == nil {
		return nil
	}

	var dbMaxID uint64
	members := make([]redis.Z, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if id > dbMaxID {
			dbMaxID = id
		}
		members = append(members, redis.Z{
			Score:  float64(id),
			Member: strconv.FormatUint(id, 10),
		})
	}

	pipe := f.rdb.Pipeline()
	pipe.Del(ctx, latestFeedTmpKey)
	if len(members) > 0 {
		pipe.ZAdd(ctx, latestFeedTmpKey, members...)
		pipe.Expire(ctx, latestFeedTmpKey, latestFeedTmpTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("latest feed reconcile stage tmp: %w", err)
	}

	if err := latestFeedReconcileSwapScript.Run(
		ctx,
		f.rdb,
		[]string{latestFeedTmpKey, latestFeedKey, latestFeedReadyKey},
		strconv.FormatUint(dbMaxID, 10),
		f.maxSize,
	).Err(); err != nil {
		return fmt.Errorf("latest feed reconcile swap: %w", err)
	}
	return nil
}
