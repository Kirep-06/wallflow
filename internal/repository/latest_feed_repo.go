package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	KeyLatest        = "feed:latest"
	KeyLatestReady   = "feed:latest:ready"
	KeyLatestLock    = "feed:latest:rebuild_lock"
	keyLatestTmp     = "feed:latest:tmp"
	latestLockExpire = 30 * time.Second
)

// addLatestScript：只有热窗口已从 MySQL 构建完成（ready 存在）时才写入，避免产生“残缺 ZSet”。
// KEYS[1]=feed:latest KEYS[2]=feed:latest:ready ARGV[1]=hotSize ARGV[2..]=post_id
var addLatestScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
for i = 2, #ARGV do
  redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i])
end
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[1]) + 1))
return 1
`)

// unlockScript：只删除自己持有的锁。
var unlockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

// LatestFeedRepo 维护 Redis 中最新流的热窗口（最新 hotSize 条帖子 ID）。
// member = score = post_id；ID 为自增 BIGINT（< 2^53），可无损作为 float64 score。
type LatestFeedRepo struct {
	rdb     redis.UniversalClient
	hotSize int
}

func NewLatestFeedRepo(rdb redis.UniversalClient, hotSize int) *LatestFeedRepo {
	return &LatestFeedRepo{rdb: rdb, hotSize: hotSize}
}

func (r *LatestFeedRepo) HotSize() int { return r.hotSize }

// Add 把帖子写入热窗口并裁剪；返回 false 表示热窗口未就绪而跳过。
func (r *LatestFeedRepo) Add(ctx context.Context, ids ...uint64) (bool, error) {
	if len(ids) == 0 {
		return true, nil
	}
	args := make([]any, 0, len(ids)+1)
	args = append(args, r.hotSize)
	for _, id := range ids {
		args = append(args, id)
	}
	n, err := addLatestScript.Run(ctx, r.rdb, []string{KeyLatest, KeyLatestReady}, args...).Int()
	if err != nil {
		return false, fmt.Errorf("add latest feed: %w", err)
	}
	return n == 1, nil
}

// RangeResult 是一次热窗口读取的结果。
type RangeResult struct {
	Ready bool     // 热窗口是否已构建
	IDs   []uint64 // id < beforeID 的倒序 ID
	Card  int64    // 热窗口当前大小
}

// Range 一次往返读取 ready 标记、id < beforeID 的前 count 条 ID 以及 ZCARD；beforeID 为 0 表示从最新开始。
func (r *LatestFeedRepo) Range(ctx context.Context, beforeID uint64, count int) (*RangeResult, error) {
	maxScore := "+inf"
	if beforeID > 0 {
		maxScore = "(" + strconv.FormatUint(beforeID, 10)
	}
	pipe := r.rdb.Pipeline()
	readyCmd := pipe.Exists(ctx, KeyLatestReady)
	rangeCmd := pipe.ZRevRangeByScore(ctx, KeyLatest, &redis.ZRangeBy{Max: maxScore, Min: "-inf", Count: int64(count)})
	cardCmd := pipe.ZCard(ctx, KeyLatest)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("range latest feed: %w", err)
	}
	ids, err := parseIDs(rangeCmd.Val())
	if err != nil {
		return nil, err
	}
	return &RangeResult{Ready: readyCmd.Val() == 1, IDs: ids, Card: cardCmd.Val()}, nil
}

// MinID 返回热窗口中最小的帖子 ID（冷热分界）；热窗口为空或未就绪时返回 0。
func (r *LatestFeedRepo) MinID(ctx context.Context) (uint64, error) {
	return r.edgeID(ctx, false)
}

// MaxID 返回热窗口中最大的帖子 ID；热窗口为空时返回 0。
func (r *LatestFeedRepo) MaxID(ctx context.Context) (uint64, error) {
	return r.edgeID(ctx, true)
}

func (r *LatestFeedRepo) edgeID(ctx context.Context, max bool) (uint64, error) {
	var vals []string
	var err error
	if max {
		vals, err = r.rdb.ZRevRange(ctx, KeyLatest, 0, 0).Result()
	} else {
		vals, err = r.rdb.ZRange(ctx, KeyLatest, 0, 0).Result()
	}
	if err != nil {
		return 0, fmt.Errorf("latest feed edge: %w", err)
	}
	if len(vals) == 0 {
		return 0, nil
	}
	return strconv.ParseUint(vals[0], 10, 64)
}

func (r *LatestFeedRepo) IsReady(ctx context.Context) (bool, error) {
	n, err := r.rdb.Exists(ctx, KeyLatestReady).Result()
	return n == 1, err
}

// TryLock 抢占重建锁，返回锁令牌；未抢到返回空串。
func (r *LatestFeedRepo) TryLock(ctx context.Context) (string, error) {
	token := strconv.FormatInt(time.Now().UnixNano(), 36)
	ok, err := r.rdb.SetNX(ctx, KeyLatestLock, token, latestLockExpire).Result()
	if err != nil || !ok {
		return "", err
	}
	return token, nil
}

func (r *LatestFeedRepo) Unlock(ctx context.Context, token string) error {
	return unlockScript.Run(ctx, r.rdb, []string{KeyLatestLock}, token).Err()
}

// Replace 用给定 ID 原子替换热窗口（写临时 key 后 RENAME），并设置 ready 标记。
func (r *LatestFeedRepo) Replace(ctx context.Context, ids []uint64) error {
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, keyLatestTmp)
		if len(ids) == 0 {
			pipe.Del(ctx, KeyLatest)
		} else {
			members := make([]redis.Z, len(ids))
			for i, id := range ids {
				members[i] = redis.Z{Score: float64(id), Member: id}
			}
			pipe.ZAdd(ctx, keyLatestTmp, members...)
			pipe.Rename(ctx, keyLatestTmp, KeyLatest)
		}
		pipe.Set(ctx, KeyLatestReady, 1, 0)
		return nil
	})
	if err != nil {
		return fmt.Errorf("replace latest feed: %w", err)
	}
	return nil
}

func parseIDs(vals []string) ([]uint64, error) {
	ids := make([]uint64, len(vals))
	for i, v := range vals {
		id, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse feed member %q: %w", v, err)
		}
		ids[i] = id
	}
	return ids, nil
}
