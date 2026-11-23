package cache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/model"
)

const (
	// HotRankSentinel 为 rank:hot:top 永存哨兵（score=0），防空榜销毁与穿透 MySQL。
	HotRankSentinel = "0"

	hotRankKey       = "rank:hot:top"
	hotRankVerKey    = "rank:hot:version"
	hotRankTmpPrefix = "rank:hot:top:tmp:"
	hotRankTmpTTL    = 60 * time.Second
)

var (
	// hotRankSwapScript 原子切换热榜快照、清除临时 Key 继承的 TTL 并单调推进快照版本号。
	// KEYS = [tmpKey, rank:hot:top, rank:hot:version]
	// ARGV = [nowUnixMilli]
	hotRankSwapScript = redis.NewScript(`
redis.call('RENAME', KEYS[1], KEYS[2])
redis.call('PERSIST', KEYS[2])
local cur = tonumber(redis.call('GET', KEYS[3])) or 0
local nextVer = tonumber(ARGV[1]) or 0
if nextVer <= cur then
	nextVer = cur + 1
end
local formatted = string.format('%.0f', nextVer)
redis.call('SET', KEYS[3], formatted)
return formatted
`)

	// hotRankReadScript 单 RTT 原子校验就绪态、首屏版本命中零载荷短路，或读取全量快照与版本号。
	// KEYS = [rank:hot:top, rank:hot:version]
	// ARGV = [candidateVersion]（非首屏或无匹配 ETag 传 ""；通配符传 "*"）
	hotRankReadScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
	return {0}
end
local ver = redis.call('GET', KEYS[2])
if not ver then
	ver = ''
end
if ver ~= '' and (ARGV[1] == '*' or (ARGV[1] ~= '' and ARGV[1] == ver)) then
	return {2, ver}
end
local members = redis.call('ZREVRANGE', KEYS[1], 0, -1, 'WITHSCORES')
return {1, ver, members}
`)

	// hotRankRemoveScript 软删壁纸时若该壁纸在榜则移除并递增版本号，即时击穿旧 ETag 304。
	// KEYS = [rank:hot:top, rank:hot:version]
	// ARGV = [wallpaperID]
	hotRankRemoveScript = redis.NewScript(`
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 1 and redis.call('EXISTS', KEYS[2]) == 1 then
	redis.call('INCR', KEYS[2])
end
return removed
`)
)

// HotRankSnapshot 表示从 Redis 单 RTT 原子读取的热榜快照结果。
type HotRankSnapshot struct {
	Ready       bool
	NotModified bool
	Version     string
	Entries     []model.HotRankEntry
}

// HotRank 封装 L2 Redis 全站热门榜 Top 100 快照（rank:hot:top + rank:hot:version）与原子切换脚本。
type HotRank struct {
	rdb     *redis.Client
	nowFunc func() time.Time
}

// NewHotRank 构造热门榜 Redis 缓存组件。
func NewHotRank(rdb *redis.Client) *HotRank {
	return &HotRank{
		rdb:     rdb,
		nowFunc: time.Now,
	}
}

// Swap 将新算出的 Top 100（附带哨兵 "0"）写入唯一临时 Key，再经 Lua 原子 RENAME + PERSIST 并推进版本号。
func (h *HotRank) Swap(ctx context.Context, entries []model.HotRankEntry) (string, error) {
	if h == nil || h.rdb == nil {
		return "", nil
	}

	limit := min(len(entries), model.HotRankMaxSize)
	members := make([]redis.Z, 0, limit+1)
	members = append(members, redis.Z{Score: 0, Member: HotRankSentinel})
	for _, e := range entries {
		if e.WallpaperID == 0 || math.IsNaN(e.Score) || math.IsInf(e.Score, 0) || e.Score <= 0 {
			continue
		}
		members = append(members, redis.Z{
			Score:  e.Score,
			Member: strconv.FormatUint(e.WallpaperID, 10),
		})
		if len(members) >= model.HotRankMaxSize+1 {
			break
		}
	}

	tmpKey := hotRankTmpPrefix + strconv.FormatUint(rand.Uint64(), 36)
	pipe := h.rdb.Pipeline()
	pipe.ZAdd(ctx, tmpKey, members...)
	pipe.Expire(ctx, tmpKey, hotRankTmpTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return "", fmt.Errorf("hot rank stage tmp: %w", err)
	}

	nowMilli := max(int64(1), h.nowFunc().UTC().UnixMilli())
	ver, err := hotRankSwapScript.Run(
		ctx,
		h.rdb,
		[]string{tmpKey, hotRankKey, hotRankVerKey},
		strconv.FormatInt(nowMilli, 10),
	).Text()
	if err != nil {
		return "", fmt.Errorf("hot rank swap: %w", err)
	}
	return ver, nil
}

// ReadSnapshot 单 RTT 原子读取热榜就绪态与快照；若首屏 candidateVersion 命中则零载荷返回 NotModified=true。
func (h *HotRank) ReadSnapshot(ctx context.Context, candidateVersion string) (HotRankSnapshot, error) {
	if h == nil || h.rdb == nil {
		return HotRankSnapshot{}, nil
	}

	res, err := hotRankReadScript.Run(
		ctx,
		h.rdb,
		[]string{hotRankKey, hotRankVerKey},
		candidateVersion,
	).Slice()
	if err != nil {
		return HotRankSnapshot{}, fmt.Errorf("hot rank read snapshot: %w", err)
	}
	if len(res) == 0 {
		return HotRankSnapshot{}, nil
	}

	status, ok := res[0].(int64)
	if !ok || status == 0 {
		return HotRankSnapshot{}, nil
	}

	var ver string
	if len(res) > 1 {
		if s, ok := res[1].(string); ok {
			ver = s
		}
	}
	if status == 2 {
		return HotRankSnapshot{
			Ready:       true,
			NotModified: true,
			Version:     ver,
		}, nil
	}

	if len(res) < 3 {
		return HotRankSnapshot{Ready: true, Version: ver, Entries: []model.HotRankEntry{}}, nil
	}
	rawPairs, ok := res[2].([]any)
	if !ok {
		return HotRankSnapshot{}, errors.New("hot rank read snapshot: unexpected members type")
	}

	entries := make([]model.HotRankEntry, 0, len(rawPairs)/2)
	for i := 0; i+1 < len(rawPairs); i += 2 {
		memberStr, ok1 := rawPairs[i].(string)
		scoreStr, ok2 := rawPairs[i+1].(string)
		if !ok1 || !ok2 || memberStr == HotRankSentinel {
			continue
		}
		id, err := strconv.ParseUint(memberStr, 10, 64)
		if err != nil || id == 0 {
			continue
		}
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil || math.IsNaN(score) || math.IsInf(score, 0) || score <= 0 {
			continue
		}
		entries = append(entries, model.HotRankEntry{
			WallpaperID: id,
			Score:       score,
		})
	}
	return HotRankSnapshot{
		Ready:   true,
		Version: ver,
		Entries: entries,
	}, nil
}

// Remove 从热榜中移除已软删壁纸；若该壁纸原本在榜则同步递增快照版本号击穿旧 304。
func (h *HotRank) Remove(ctx context.Context, wallpaperID uint64) error {
	if h == nil || h.rdb == nil || wallpaperID == 0 {
		return nil
	}
	if err := hotRankRemoveScript.Run(
		ctx,
		h.rdb,
		[]string{hotRankKey, hotRankVerKey},
		strconv.FormatUint(wallpaperID, 10),
	).Err(); err != nil {
		return fmt.Errorf("hot rank remove: %w", err)
	}
	return nil
}
