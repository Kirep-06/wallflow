package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// 关注流收/发件箱与大 V 集合缓存常量。
const (
	InboxMaxSize  = 500
	OutboxMaxSize = 200
	FeedBoxTTL    = 7 * 24 * time.Hour
	BigVL1TTL     = 30 * time.Second
	FeedSentinel  = "0"

	bigVKey         = "feed:bigvs"
	bigVTmpKey      = "feed:bigvs:tmp"
	bigVTmpTTL      = 60 * time.Second
	inboxKeyPrefix  = "feed:inbox:"
	outboxKeyPrefix = "feed:outbox:"
)

var (
	// KEYS = [feed:bigvs:tmp, feed:bigvs]
	bigVReconcileSwapScript = redis.NewScript(`
redis.call('RENAME', KEYS[1], KEYS[2])
redis.call('PERSIST', KEYS[2])
return 1
`)

	// KEYS = [feed:bigvs], ARGV = [authorID]
	bigVAddIfExistsScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('SADD', KEYS[1], ARGV[1])
return 1
`)

	// KEYS = [boxKey], ARGV = [wallpaperID, maxSize, ttlSec]
	// 仅向已存在的活跃收/发件箱追加，从 rank 1 起裁剪以保卫 rank 0 哨兵 "0"。
	feedBoxPushIfExistsScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local id = tonumber(ARGV[1])
local maxSize = tonumber(ARGV[2])
redis.call('ZADD', KEYS[1], id, ARGV[1])
local card = redis.call('ZCARD', KEYS[1])
if card > maxSize + 1 then
	redis.call('ZREMRANGEBYRANK', KEYS[1], 1, card - maxSize - 1)
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
`)

	// KEYS = [boxKey], ARGV = [maxScore, count, maxSize, ttlSec]
	// 开区间 '(0' 排除哨兵；已满裁剪时返回 rank 1 作为单路安全下界 floorID。
	feedBoxReadScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return {0} end
local ids = redis.call('ZREVRANGEBYSCORE', KEYS[1], ARGV[1], '(0', 'LIMIT', 0, tonumber(ARGV[2]))
local card = redis.call('ZCARD', KEYS[1])
local floorID = '0'
if card > tonumber(ARGV[3]) then
	local oldest = redis.call('ZRANGE', KEYS[1], 1, 1)
	if #oldest > 0 then floorID = oldest[1] end
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
return {1, ids, floorID}
`)

	// KEYS = [boxKey], ARGV = [maxSize, ttlSec, id1, id2, ...]
	feedBoxRebuildScript = redis.NewScript(`
local maxSize = tonumber(ARGV[1])
local ttlSec = tonumber(ARGV[2])
redis.call('ZADD', KEYS[1], 0, '0')
for i = 3, #ARGV do
	local id = tonumber(ARGV[i])
	if id ~= nil and id > 0 then
		redis.call('ZADD', KEYS[1], id, ARGV[i])
	end
end
local card = redis.call('ZCARD', KEYS[1])
if card > maxSize + 1 then
	redis.call('ZREMRANGEBYRANK', KEYS[1], 1, card - maxSize - 1)
end
redis.call('EXPIRE', KEYS[1], ttlSec)
return 1
`)
)

// BigVSet 封装全站大 V 集合（L2 永不过期 feed:bigvs 含哨兵 "0" + API 30s L1 内存只读快照）。
type BigVSet struct {
	rdb     *redis.Client
	l1TTL   time.Duration
	nowFunc func() time.Time

	mu       sync.RWMutex
	snapshot map[uint64]struct{}
	expireAt time.Time
	sf       singleflight.Group
}

// NewBigVSet 构造全站大 V 集合缓存组件（l1TTL <= 0 时默认 BigVL1TTL）。
func NewBigVSet(rdb *redis.Client, l1TTL time.Duration) *BigVSet {
	if l1TTL <= 0 {
		l1TTL = BigVL1TTL
	}
	return &BigVSet{rdb: rdb, l1TTL: l1TTL, nowFunc: time.Now}
}

// Reconcile 使用 MySQL 全量大 V ID 原子对账切换 feed:bigvs（始终含哨兵 "0" 且永不过期）。
func (b *BigVSet) Reconcile(ctx context.Context, ids []uint64) error {
	if b.rdb == nil {
		return nil
	}

	members := make([]any, 0, len(ids)+1)
	members = append(members, FeedSentinel)
	for _, id := range ids {
		if id > 0 {
			members = append(members, strconv.FormatUint(id, 10))
		}
	}

	pipe := b.rdb.Pipeline()
	pipe.Del(ctx, bigVTmpKey)
	pipe.SAdd(ctx, bigVTmpKey, members...)
	pipe.Expire(ctx, bigVTmpKey, bigVTmpTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("bigv set stage tmp: %w", err)
	}
	if err := bigVReconcileSwapScript.Run(ctx, b.rdb, []string{bigVTmpKey, bigVKey}).Err(); err != nil {
		return fmt.Errorf("bigv set reconcile swap: %w", err)
	}
	return nil
}

// AddIfExists 在 feed:bigvs 已对账就绪时增量加入新晋升大 V，并立即失效本进程 L1 快照。
func (b *BigVSet) AddIfExists(ctx context.Context, userID uint64) error {
	if userID == 0 {
		return nil
	}
	b.mu.Lock()
	b.snapshot = nil
	b.expireAt = time.Time{}
	b.mu.Unlock()
	b.sf.Forget(bigVKey)

	if b.rdb == nil {
		return nil
	}
	if err := bigVAddIfExistsScript.Run(ctx, b.rdb, []string{bigVKey}, strconv.FormatUint(userID, 10)).Err(); err != nil {
		return fmt.Errorf("bigv set add if exists: %w", err)
	}
	return nil
}

// Snapshot 按 L1 30s 只读快照 -> L2 feed:bigvs -> singleflight(loader) 顺序返回全站大 V 集合。
func (b *BigVSet) Snapshot(ctx context.Context, loader func(context.Context) ([]uint64, error)) (map[uint64]struct{}, error) {
	if snap, ok := b.getL1(); ok {
		return snap, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ch := b.sf.DoChan(bigVKey, func() (any, error) {
		if snap, ok := b.getL1(); ok {
			return snap, nil
		}

		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), loaderTimeout)
		defer cancel()

		redisHealthy := b.rdb != nil
		if b.rdb != nil {
			members, err := b.rdb.SMembers(loadCtx, bigVKey).Result()
			if err != nil {
				redisHealthy = false
				slog.WarnContext(loadCtx, "bigv set l2 smembers degraded to loader", slog.Any("err", err))
			} else if len(members) > 0 {
				snap := make(map[uint64]struct{}, len(members)-1)
				for _, m := range members {
					if id, err := strconv.ParseUint(m, 10, 64); err == nil && id > 0 {
						snap[id] = struct{}{}
					}
				}
				b.setL1(snap)
				return snap, nil
			}
		}

		snap := map[uint64]struct{}{}
		if loader != nil {
			ids, err := loader(loadCtx)
			if err != nil {
				return nil, err
			}
			snap = make(map[uint64]struct{}, len(ids))
			for _, id := range ids {
				if id > 0 {
					snap[id] = struct{}{}
				}
			}
			if redisHealthy {
				if recErr := b.Reconcile(loadCtx, ids); recErr != nil {
					slog.WarnContext(loadCtx, "bigv set l2 backfill failed", slog.Int("count", len(ids)), slog.Any("err", recErr))
				}
			}
		}
		b.setL1(snap)
		return snap, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		snap, ok := res.Val.(map[uint64]struct{})
		if !ok {
			return nil, errors.New("cache: unexpected bigv snapshot type")
		}
		return snap, nil
	}
}

func (b *BigVSet) getL1() (map[uint64]struct{}, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.snapshot != nil && b.nowFunc().Before(b.expireAt) {
		return b.snapshot, true
	}
	return nil, false
}

func (b *BigVSet) setL1(snap map[uint64]struct{}) {
	b.mu.Lock()
	b.snapshot = snap
	b.expireAt = b.nowFunc().Add(b.l1TTL)
	b.mu.Unlock()
}

// FeedBoxReadResult 描述单次收件箱或发件箱热窗口读取结果与安全下界水位线。
type FeedBoxReadResult struct {
	Exists  bool
	IDs     []uint64
	FloorID uint64
}

// FollowingFeed 封装活跃粉丝收件箱（feed:inbox:{uid}）与大 V 发件箱（feed:outbox:{author_id}）的 Lua 操作。
type FollowingFeed struct {
	rdb       *redis.Client
	inboxMax  int
	outboxMax int
	ttl       time.Duration
}

// NewFollowingFeed 构造关注流收/发件箱缓存组件（默认容量 500 / 200，TTL 7 天 ±10%）。
func NewFollowingFeed(rdb *redis.Client) *FollowingFeed {
	return &FollowingFeed{rdb: rdb, inboxMax: InboxMaxSize, outboxMax: OutboxMaxSize, ttl: FeedBoxTTL}
}

// ReadInbox 原子读取用户普通作者收件箱（id < beforeID，0 表示首页）及安全水位线并续期。
func (f *FollowingFeed) ReadInbox(ctx context.Context, userID, beforeID uint64, limit int) (FeedBoxReadResult, error) {
	if f.rdb == nil || userID == 0 || limit <= 0 {
		return FeedBoxReadResult{}, nil
	}
	raw, err := feedBoxReadScript.Run(ctx, f.rdb, []string{inboxKey(userID)}, maxScoreArg(beforeID), limit, f.inboxMax, f.ttlSec()).Slice()
	if err != nil {
		return FeedBoxReadResult{}, fmt.Errorf("following feed read inbox: %w", err)
	}
	return parseFeedBoxReadReply(raw)
}

// ReadOutboxes 通过单次 Redis Pipeline 批量读取多个大 V 发件箱与各自安全水位线并续期。
func (f *FollowingFeed) ReadOutboxes(ctx context.Context, authorIDs []uint64, beforeID uint64, limit int) (map[uint64]FeedBoxReadResult, error) {
	res := make(map[uint64]FeedBoxReadResult, len(authorIDs))
	if f.rdb == nil || len(authorIDs) == 0 || limit <= 0 {
		return res, nil
	}

	maxScore := maxScoreArg(beforeID)
	var cmds []*redis.Cmd
	err := f.execPipelineWithScript(ctx, feedBoxReadScript, func(pipe redis.Pipeliner) {
		cmds = make([]*redis.Cmd, len(authorIDs))
		for i, aid := range authorIDs {
			cmds[i] = feedBoxReadScript.Run(ctx, pipe, []string{outboxKey(aid)}, maxScore, limit, f.outboxMax, f.ttlSec())
		}
	})
	if err != nil {
		return nil, fmt.Errorf("following feed read outboxes pipeline: %w", err)
	}

	for i, aid := range authorIDs {
		reply, err := cmds[i].Slice()
		if err != nil {
			return nil, fmt.Errorf("following feed parse outbox %d: %w", aid, err)
		}
		parsed, err := parseFeedBoxReadReply(reply)
		if err != nil {
			return nil, err
		}
		res[aid] = parsed
	}
	return res, nil
}

// RebuildInbox 原子重建用户普通作者收件箱（强制含哨兵 "0"、保护哨兵裁剪至上限并设 7 天 ±10% TTL）。
func (f *FollowingFeed) RebuildInbox(ctx context.Context, userID uint64, ids []uint64) error {
	if f.rdb == nil || userID == 0 {
		return nil
	}
	return f.rebuildBox(ctx, inboxKey(userID), ids, f.inboxMax)
}

// RebuildOutbox 原子重建大 V 个人发件箱（强制含哨兵 "0"、保护哨兵裁剪至上限并设 7 天 ±10% TTL）。
func (f *FollowingFeed) RebuildOutbox(ctx context.Context, authorID uint64, ids []uint64) error {
	if f.rdb == nil || authorID == 0 {
		return nil
	}
	return f.rebuildBox(ctx, outboxKey(authorID), ids, f.outboxMax)
}

// BatchPushInboxIfExists 使用单次 Pipeline 批量向已存在的活跃粉丝收件箱条件追加壁纸 ID 并保哨兵裁剪。
func (f *FollowingFeed) BatchPushInboxIfExists(ctx context.Context, followerIDs []uint64, wallpaperID uint64) error {
	if f.rdb == nil || len(followerIDs) == 0 || wallpaperID == 0 {
		return nil
	}

	wpStr := strconv.FormatUint(wallpaperID, 10)
	err := f.execPipelineWithScript(ctx, feedBoxPushIfExistsScript, func(pipe redis.Pipeliner) {
		for _, uid := range followerIDs {
			if uid > 0 {
				feedBoxPushIfExistsScript.Run(ctx, pipe, []string{inboxKey(uid)}, wpStr, f.inboxMax, f.ttlSec())
			}
		}
	})
	if err != nil {
		return fmt.Errorf("following feed batch push inbox: %w", err)
	}
	return nil
}

// PushOutboxIfExists 仅在大 V 发件箱已存在时条件追加壁纸 ID 并保哨兵裁剪。
func (f *FollowingFeed) PushOutboxIfExists(ctx context.Context, authorID, wallpaperID uint64) error {
	if f.rdb == nil || authorID == 0 || wallpaperID == 0 {
		return nil
	}
	if err := feedBoxPushIfExistsScript.Run(ctx, f.rdb, []string{outboxKey(authorID)}, strconv.FormatUint(wallpaperID, 10), f.outboxMax, f.ttlSec()).Err(); err != nil {
		return fmt.Errorf("following feed push outbox: %w", err)
	}
	return nil
}

// DeleteInbox 删除指定用户的收件箱缓存（关注/取关普通作者时触发下次懒重建）。
func (f *FollowingFeed) DeleteInbox(ctx context.Context, userID uint64) error {
	if f.rdb == nil || userID == 0 {
		return nil
	}
	if err := f.rdb.Del(ctx, inboxKey(userID)).Err(); err != nil {
		return fmt.Errorf("following feed delete inbox: %w", err)
	}
	return nil
}

// RemoveFromOutbox 从大 V 发件箱移除已软删壁纸 ID（不触碰哨兵 "0"）。
func (f *FollowingFeed) RemoveFromOutbox(ctx context.Context, authorID, wallpaperID uint64) error {
	if f.rdb == nil || authorID == 0 || wallpaperID == 0 {
		return nil
	}
	if err := f.rdb.ZRem(ctx, outboxKey(authorID), strconv.FormatUint(wallpaperID, 10)).Err(); err != nil {
		return fmt.Errorf("following feed remove from outbox: %w", err)
	}
	return nil
}

func (f *FollowingFeed) execPipelineWithScript(ctx context.Context, script *redis.Script, enqueue func(pipe redis.Pipeliner)) error {
	pipe := f.rdb.Pipeline()
	enqueue(pipe)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		return err
	}
	// Pipeline 走 EVALSHA 不自动回落 EVAL，遇 Redis 冷启/重启 NOSCRIPT 时先 SCRIPT LOAD 再重放一次。
	if loadErr := script.Load(ctx, f.rdb).Err(); loadErr != nil {
		return loadErr
	}
	retryPipe := f.rdb.Pipeline()
	enqueue(retryPipe)
	_, err = retryPipe.Exec(ctx)
	return err
}

func (f *FollowingFeed) rebuildBox(ctx context.Context, key string, ids []uint64, maxSize int) error {
	args := make([]any, 0, len(ids)+2)
	args = append(args, maxSize, f.ttlSec())
	for _, id := range ids {
		if id > 0 {
			args = append(args, strconv.FormatUint(id, 10))
		}
	}
	if err := feedBoxRebuildScript.Run(ctx, f.rdb, []string{key}, args...).Err(); err != nil {
		return fmt.Errorf("following feed rebuild %s: %w", key, err)
	}
	return nil
}

func (f *FollowingFeed) ttlSec() int { return ttlToSeconds(applyJitter(f.ttl)) }
func inboxKey(uid uint64) string     { return inboxKeyPrefix + strconv.FormatUint(uid, 10) }
func outboxKey(aid uint64) string    { return outboxKeyPrefix + strconv.FormatUint(aid, 10) }
func maxScoreArg(beforeID uint64) string {
	if beforeID == 0 {
		return "+inf"
	}
	return "(" + strconv.FormatUint(beforeID, 10)
}

func parseFeedBoxReadReply(reply []any) (FeedBoxReadResult, error) {
	if len(reply) == 0 {
		return FeedBoxReadResult{}, nil
	}
	existsFlag, ok := reply[0].(int64)
	if !ok || existsFlag == 0 {
		return FeedBoxReadResult{Exists: false}, nil
	}

	out := FeedBoxReadResult{Exists: true}
	if len(reply) >= 2 {
		rawIDs, ok := reply[1].([]any)
		if !ok {
			return FeedBoxReadResult{}, errors.New("following feed read: unexpected ids reply type")
		}
		out.IDs = make([]uint64, 0, len(rawIDs))
		for _, elem := range rawIDs {
			if s, ok := elem.(string); ok {
				if id, err := strconv.ParseUint(s, 10, 64); err == nil && id > 0 {
					out.IDs = append(out.IDs, id)
				}
			}
		}
	}
	if len(reply) >= 3 {
		if floorStr, ok := reply[2].(string); ok {
			if floorID, err := strconv.ParseUint(floorStr, 10, 64); err == nil {
				out.FloorID = floorID
			}
		}
	}
	return out, nil
}
