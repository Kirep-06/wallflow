package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
)

const (
	KeyBigVs = "feed:bigvs"

	timelineSentinelMember = "0"
	bigVSentinelMember     = "0"
)

// addTimelineIfExistsScript 仅对已初始化（存在）的收件箱/发件箱追加帖子，从 rank 1 开始裁剪以保护 rank 0 哨兵 "0"，并同步续期。
// KEYS[1] = feed:inbox:{uid} 或 feed:outbox:{author_id}
// ARGV[1] = hot_size (不含哨兵，如 500 或 200)
// ARGV[2] = ttl_seconds (带 ±10% 抖动)
// ARGV[3..] = 待追加的一个或多个 post_id (>= 1)
var addTimelineIfExistsScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
for i = 3, #ARGV do
  redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i])
end
redis.call('ZREMRANGEBYRANK', KEYS[1], 1, -(tonumber(ARGV[1]) + 1))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
`)

// addBigVIfExistsScript 仅当全站大 V 集合已完成初始化（含哨兵 "0"）时才追加新大 V，防止产生残缺集合。
// KEYS[1] = feed:bigvs
// ARGV[1] = author_id
var addBigVIfExistsScript = redis.NewScript(`
if redis.call('SISMEMBER', KEYS[1], '0') == 0 then
  return 0
end
redis.call('SADD', KEYS[1], ARGV[1])
return 1
`)

// TimelineRangeResult 表示单路 ZSet（收件箱或大 V 发件箱）的 Pipeline 读取结果。
type TimelineRangeResult struct {
	Exists bool     // Key 是否已初始化
	IDs    []uint64 // id < beforeID 且 > 0 的倒序真实帖子 ID
	Card   int64    // ZCARD（包含哨兵 0）
	MinID  uint64   // 热窗口内最旧真实帖子 ID（rank 1）；无真实帖子时为 0
}

// FollowingFeedRepo 维护 Redis 中关注流收件箱（feed:inbox:*）、大 V 发件箱（feed:outbox:*）与全站大 V 集合（feed:bigvs）。
type FollowingFeedRepo struct {
	rdb           redis.UniversalClient
	inboxHotSize  int
	outboxHotSize int
	inboxTTL      time.Duration
	outboxTTL     time.Duration
	tmpSeq        atomic.Uint64
}

func NewFollowingFeedRepo(rdb redis.UniversalClient, cfg config.FollowingFeedConfig) *FollowingFeedRepo {
	inboxHot := cfg.InboxHotSize
	if inboxHot <= 0 {
		inboxHot = 500
	}
	outboxHot := cfg.OutboxHotSize
	if outboxHot <= 0 {
		outboxHot = 200
	}
	inboxTTL := time.Duration(cfg.InboxTTLSeconds) * time.Second
	if inboxTTL <= 0 {
		inboxTTL = 7 * 24 * time.Hour
	}
	outboxTTL := time.Duration(cfg.OutboxTTLSeconds) * time.Second
	if outboxTTL <= 0 {
		outboxTTL = 7 * 24 * time.Hour
	}
	return &FollowingFeedRepo{
		rdb:           rdb,
		inboxHotSize:  inboxHot,
		outboxHotSize: outboxHot,
		inboxTTL:      inboxTTL,
		outboxTTL:     outboxTTL,
	}
}

func (r *FollowingFeedRepo) InboxHotSize() int  { return r.inboxHotSize }
func (r *FollowingFeedRepo) OutboxHotSize() int { return r.outboxHotSize }

func InboxKey(uid uint64) string {
	return "feed:inbox:" + strconv.FormatUint(uid, 10)
}

func OutboxKey(authorID uint64) string {
	return "feed:outbox:" + strconv.FormatUint(authorID, 10)
}

func (r *FollowingFeedRepo) nextNonce() string {
	seq := r.tmpSeq.Add(1)
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatUint(seq, 36)
}

func (r *FollowingFeedRepo) jitteredTTL(base time.Duration) time.Duration {
	d := cache.Jitter(base)
	if d < time.Second {
		return time.Second
	}
	return d
}

func (r *FollowingFeedRepo) jitteredTTLSeconds(base time.Duration) int64 {
	sec := int64(r.jitteredTTL(base) / time.Second)
	if sec < 1 {
		return 1
	}
	return sec
}

type timelinePipeCmds struct {
	existsCmd *redis.IntCmd
	rangeCmd  *redis.StringSliceCmd
	cardCmd   *redis.IntCmd
	minCmd    *redis.StringSliceCmd
}

func (r *FollowingFeedRepo) enqueueTimelineRange(ctx context.Context, pipe redis.Pipeliner, key string, maxScore string, count int, ttl time.Duration) timelinePipeCmds {
	cmds := timelinePipeCmds{
		existsCmd: pipe.Exists(ctx, key),
		rangeCmd:  pipe.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{Max: maxScore, Min: "(0", Count: int64(count)}),
		cardCmd:   pipe.ZCard(ctx, key),
		minCmd:    pipe.ZRange(ctx, key, 1, 1),
	}
	// 对不存在 Key 执行 EXPIRE 仅返回 0，不会创建残缺 Key。
	pipe.Expire(ctx, key, r.jitteredTTL(ttl))
	return cmds
}

func parseTimelinePipeCmds(cmds timelinePipeCmds) (*TimelineRangeResult, error) {
	exists := cmds.existsCmd.Val() == 1
	if !exists {
		return &TimelineRangeResult{Exists: false}, nil
	}
	ids, err := parseIDs(cmds.rangeCmd.Val())
	if err != nil {
		return nil, err
	}
	var minID uint64
	if minVals := cmds.minCmd.Val(); len(minVals) > 0 {
		minID, err = strconv.ParseUint(minVals[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse timeline min member %q: %w", minVals[0], err)
		}
	}
	return &TimelineRangeResult{
		Exists: true,
		IDs:    ids,
		Card:   cmds.cardCmd.Val(),
		MinID:  minID,
	}, nil
}

// RangeInbox 读取单用户收件箱热窗口（供建箱前双重检查与单测使用）。
func (r *FollowingFeedRepo) RangeInbox(ctx context.Context, userID, beforeID uint64, count int) (*TimelineRangeResult, error) {
	inboxRes, _, err := r.RangeInboxAndOutboxes(ctx, userID, true, nil, beforeID, count)
	return inboxRes, err
}

// RangeOutbox 读取单个大 V 发件箱热窗口。
func (r *FollowingFeedRepo) RangeOutbox(ctx context.Context, authorID, beforeID uint64, count int) (*TimelineRangeResult, error) {
	_, outboxes, err := r.RangeInboxAndOutboxes(ctx, 0, false, []uint64{authorID}, beforeID, count)
	if err != nil {
		return nil, err
	}
	return outboxes[authorID], nil
}

// RangeInboxAndOutboxes 通过单次 Redis Pipeline 批量读取普通作者收件箱与多个大 V 发件箱，并惰性续期 7 天活跃期。
func (r *FollowingFeedRepo) RangeInboxAndOutboxes(
	ctx context.Context,
	userID uint64,
	readInbox bool,
	bigVIDs []uint64,
	beforeID uint64,
	count int,
) (*TimelineRangeResult, map[uint64]*TimelineRangeResult, error) {
	outboxes := make(map[uint64]*TimelineRangeResult, len(bigVIDs))
	if !readInbox && len(bigVIDs) == 0 {
		return nil, outboxes, nil
	}

	maxScore := "+inf"
	if beforeID > 0 {
		maxScore = "(" + strconv.FormatUint(beforeID, 10)
	}

	pipe := r.rdb.Pipeline()
	var inboxCmds timelinePipeCmds
	if readInbox {
		inboxCmds = r.enqueueTimelineRange(ctx, pipe, InboxKey(userID), maxScore, count, r.inboxTTL)
	}
	outboxCmds := make(map[uint64]timelinePipeCmds, len(bigVIDs))
	for _, bid := range bigVIDs {
		if _, dup := outboxCmds[bid]; dup {
			continue
		}
		outboxCmds[bid] = r.enqueueTimelineRange(ctx, pipe, OutboxKey(bid), maxScore, count, r.outboxTTL)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, nil, fmt.Errorf("range following timelines: %w", err)
	}

	var inboxRes *TimelineRangeResult
	var err error
	if readInbox {
		inboxRes, err = parseTimelinePipeCmds(inboxCmds)
		if err != nil {
			return nil, nil, err
		}
	}
	for bid, cmds := range outboxCmds {
		res, err := parseTimelinePipeCmds(cmds)
		if err != nil {
			return nil, nil, err
		}
		outboxes[bid] = res
	}
	return inboxRes, outboxes, nil
}

// ReplaceInbox 原子构建并替换用户收件箱（必写哨兵 "0"，带 7 天 ±10% TTL）。
func (r *FollowingFeedRepo) ReplaceInbox(ctx context.Context, userID uint64, ids []uint64) error {
	targetKey := InboxKey(userID)
	tmpKey := fmt.Sprintf("feed:inbox:tmp:%d:%s", userID, r.nextNonce())
	return r.replaceTimeline(ctx, targetKey, tmpKey, ids, r.inboxHotSize, r.inboxTTL)
}

// ReplaceOutbox 原子构建并替换大 V 发件箱（必写哨兵 "0"，带 7 天 ±10% TTL）。
func (r *FollowingFeedRepo) ReplaceOutbox(ctx context.Context, authorID uint64, ids []uint64) error {
	targetKey := OutboxKey(authorID)
	tmpKey := fmt.Sprintf("feed:outbox:tmp:%d:%s", authorID, r.nextNonce())
	return r.replaceTimeline(ctx, targetKey, tmpKey, ids, r.outboxHotSize, r.outboxTTL)
}

func (r *FollowingFeedRepo) replaceTimeline(ctx context.Context, targetKey, tmpKey string, ids []uint64, hotSize int, baseTTL time.Duration) error {
	if len(ids) > hotSize {
		ids = ids[:hotSize]
	}
	members := make([]redis.Z, 0, len(ids)+1)
	members = append(members, redis.Z{Score: 0, Member: timelineSentinelMember})
	for _, id := range ids {
		if id == 0 {
			continue
		}
		members = append(members, redis.Z{Score: float64(id), Member: strconv.FormatUint(id, 10)})
	}

	ttl := r.jitteredTTL(baseTTL)
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, tmpKey)
		pipe.ZAdd(ctx, tmpKey, members...)
		pipe.Expire(ctx, tmpKey, ttl)
		pipe.Rename(ctx, tmpKey, targetKey)
		return nil
	})
	if err != nil {
		return fmt.Errorf("replace timeline %s: %w", targetKey, err)
	}
	return nil
}

// AddToInboxIfExists 条件向已存在的用户收件箱追加帖子并保哨兵裁剪、续期；Key 不存在返回 false 且不创建。
func (r *FollowingFeedRepo) AddToInboxIfExists(ctx context.Context, userID uint64, postIDs ...uint64) (bool, error) {
	return r.addTimelineIfExists(ctx, InboxKey(userID), r.inboxHotSize, r.inboxTTL, postIDs...)
}

// AddToOutboxIfExists 条件向已存在的大 V 发件箱追加帖子并保哨兵裁剪、续期；Key 不存在返回 false 且不创建。
func (r *FollowingFeedRepo) AddToOutboxIfExists(ctx context.Context, authorID uint64, postIDs ...uint64) (bool, error) {
	return r.addTimelineIfExists(ctx, OutboxKey(authorID), r.outboxHotSize, r.outboxTTL, postIDs...)
}

func (r *FollowingFeedRepo) addTimelineIfExists(ctx context.Context, key string, hotSize int, baseTTL time.Duration, postIDs ...uint64) (bool, error) {
	args := make([]any, 0, len(postIDs)+2)
	args = append(args, hotSize, r.jitteredTTLSeconds(baseTTL))
	for _, id := range postIDs {
		if id > 0 {
			args = append(args, strconv.FormatUint(id, 10))
		}
	}
	if len(args) == 2 {
		return true, nil
	}
	n, err := addTimelineIfExistsScript.Run(ctx, r.rdb, []string{key}, args...).Int()
	if err != nil {
		return false, fmt.Errorf("add timeline %s: %w", key, err)
	}
	return n == 1, nil
}

// BatchAddToInboxesIfExists 通过单次 Redis Pipeline 向一批粉丝的活跃收件箱条件写入同一帖子。
// 若 Redis 重启导致脚本未缓存（NOSCRIPT），自动执行 SCRIPT LOAD 后重试一次（脚本纯幂等）。
func (r *FollowingFeedRepo) BatchAddToInboxesIfExists(ctx context.Context, followerIDs []uint64, postID uint64) error {
	if postID == 0 || len(followerIDs) == 0 {
		return nil
	}
	if err := r.execBatchInboxScript(ctx, followerIDs, postID); err != nil {
		if strings.Contains(err.Error(), "NOSCRIPT") {
			if loadErr := addTimelineIfExistsScript.Load(ctx, r.rdb).Err(); loadErr != nil {
				return fmt.Errorf("load addTimelineIfExistsScript: %w", loadErr)
			}
			return r.execBatchInboxScript(ctx, followerIDs, postID)
		}
		return err
	}
	return nil
}

func (r *FollowingFeedRepo) execBatchInboxScript(ctx context.Context, followerIDs []uint64, postID uint64) error {
	postStr := strconv.FormatUint(postID, 10)
	pipe := r.rdb.Pipeline()
	count := 0
	for _, fid := range followerIDs {
		if fid == 0 {
			continue
		}
		addTimelineIfExistsScript.Run(ctx, pipe, []string{InboxKey(fid)}, r.inboxHotSize, r.jitteredTTLSeconds(r.inboxTTL), postStr)
		count++
	}
	if count == 0 {
		return nil
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("batch add to inboxes for post %d: %w", postID, err)
	}
	return nil
}

// DeleteInbox 删除指定用户的收件箱热窗口（关注/取关普通作者时触发失效）。
func (r *FollowingFeedRepo) DeleteInbox(ctx context.Context, userID uint64) error {
	if userID == 0 {
		return nil
	}
	if err := r.rdb.Del(ctx, InboxKey(userID)).Err(); err != nil {
		return fmt.Errorf("delete inbox %d: %w", userID, err)
	}
	return nil
}

// GetBigVs 读取全站大 V 集合；仅当集合包含哨兵 "0" 时视为已完整初始化，避免把残缺集合当成全集。
func (r *FollowingFeedRepo) GetBigVs(ctx context.Context) (map[uint64]struct{}, bool, error) {
	vals, err := r.rdb.SMembers(ctx, KeyBigVs).Result()
	if err != nil {
		return nil, false, fmt.Errorf("get bigvs: %w", err)
	}
	if len(vals) == 0 {
		return nil, false, nil
	}
	out := make(map[uint64]struct{}, len(vals))
	hasSentinel := false
	for _, v := range vals {
		if v == bigVSentinelMember {
			hasSentinel = true
			continue
		}
		id, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, false, fmt.Errorf("parse bigv member %q: %w", v, err)
		}
		if id > 0 {
			out[id] = struct{}{}
		}
	}
	if !hasSentinel {
		return nil, false, nil
	}
	return out, true, nil
}

// AddBigVIfExists 当全站大 V 集合已初始化时，追加新晋升的大 V ID。
func (r *FollowingFeedRepo) AddBigVIfExists(ctx context.Context, authorID uint64) (bool, error) {
	if authorID == 0 {
		return false, nil
	}
	n, err := addBigVIfExistsScript.Run(ctx, r.rdb, []string{KeyBigVs}, strconv.FormatUint(authorID, 10)).Int()
	if err != nil {
		return false, fmt.Errorf("add bigv %d: %w", authorID, err)
	}
	return n == 1, nil
}

// ReplaceBigVs 用最新大 V 列表原子替换全站集合（始终保留哨兵 "0"，永不过期）。
func (r *FollowingFeedRepo) ReplaceBigVs(ctx context.Context, bigVIDs []uint64) error {
	tmpKey := fmt.Sprintf("feed:bigvs:tmp:%s", r.nextNonce())
	members := make([]any, 0, len(bigVIDs)+1)
	members = append(members, bigVSentinelMember)
	for _, id := range bigVIDs {
		if id > 0 {
			members = append(members, strconv.FormatUint(id, 10))
		}
	}
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, tmpKey)
		pipe.SAdd(ctx, tmpKey, members...)
		pipe.Rename(ctx, tmpKey, KeyBigVs)
		return nil
	})
	if err != nil {
		return fmt.Errorf("replace bigvs: %w", err)
	}
	return nil
}
