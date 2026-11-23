package service

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
)

const (
	maxNicknameRunes      = 32
	followStatsKeyPrefix  = "follow:stats:"
	followingIDsKeyPrefix = "feed:following_ids:"
)

func validateNickname(raw string) (string, error) {
	nick := strings.TrimSpace(raw)
	n := utf8.RuneCountInString(nick)
	if n < 1 || n > maxNicknameRunes {
		return "", errno.ErrInvalidParam.WithMessage("昵称长度需为 1~32 个字符")
	}
	return nick, nil
}

func normalizeListLimit(limit int) (int, error) {
	if limit == 0 {
		return cursor.DefaultLimit, nil
	}
	if limit < cursor.MinLimit || limit > cursor.MaxLimit {
		return 0, errno.ErrInvalidParam.WithMessage("limit 参数不合法")
	}
	return limit, nil
}

func parseIDCursor(raw string) (uint64, error) {
	if raw == "" {
		return 0, nil
	}
	c, err := cursor.Decode[cursor.IDCursor](raw)
	if err != nil {
		return 0, err
	}
	if c.ID == 0 {
		return 0, cursor.ErrInvalid
	}
	return c.ID, nil
}

func followStatsKey(userID uint64) string {
	return followStatsKeyPrefix + strconv.FormatUint(userID, 10)
}

func followingIDsKey(userID uint64) string {
	return followingIDsKeyPrefix + strconv.FormatUint(userID, 10)
}

func getFollowStats(
	ctx context.Context,
	statsCache *cache.MultiLevel[model.FollowStatsCache],
	followRepo *repository.FollowRepository,
	userID uint64,
) (model.FollowStatsCache, error) {
	if userID == 0 {
		return model.FollowStatsCache{}, nil
	}
	if statsCache == nil {
		if followRepo == nil {
			return model.FollowStatsCache{}, nil
		}
		stats, err := followRepo.FindStatsByUserID(ctx, userID)
		if err != nil {
			return model.FollowStatsCache{}, errno.ErrInternal.Wrap(err)
		}
		return stats, nil
	}

	stats, err := statsCache.Get(ctx, followStatsKey(userID), func(loadCtx context.Context) (model.FollowStatsCache, error) {
		if followRepo == nil {
			return model.FollowStatsCache{}, nil
		}
		return followRepo.FindStatsByUserID(loadCtx, userID)
	})
	if err != nil {
		return model.FollowStatsCache{}, errno.ErrInternal.Wrap(err)
	}
	return stats, nil
}

func mgetFollowStats(
	ctx context.Context,
	statsCache *cache.MultiLevel[model.FollowStatsCache],
	followRepo *repository.FollowRepository,
	userIDs []uint64,
) (map[uint64]model.FollowStatsCache, error) {
	out := make(map[uint64]model.FollowStatsCache, len(userIDs))
	if len(userIDs) == 0 {
		return out, nil
	}

	keys := make([]string, 0, len(userIDs))
	keyToID := make(map[string]uint64, len(userIDs))
	uniqueIDs := make([]uint64, 0, len(userIDs))
	for _, uid := range userIDs {
		if uid == 0 {
			continue
		}
		k := followStatsKey(uid)
		if _, dup := keyToID[k]; dup {
			continue
		}
		keyToID[k] = uid
		keys = append(keys, k)
		uniqueIDs = append(uniqueIDs, uid)
	}
	if len(keys) == 0 {
		return out, nil
	}

	if statsCache == nil {
		if followRepo == nil {
			return out, nil
		}
		found, err := followRepo.FindStatsByUserIDs(ctx, uniqueIDs)
		if err != nil {
			return nil, errno.ErrInternal.Wrap(err)
		}
		return found, nil
	}

	cached, err := statsCache.MGet(ctx, keys, func(loadCtx context.Context, missKeys []string) (map[string]model.FollowStatsCache, error) {
		missIDs := make([]uint64, 0, len(missKeys))
		for _, k := range missKeys {
			if uid, ok := keyToID[k]; ok {
				missIDs = append(missIDs, uid)
			}
		}
		res := make(map[string]model.FollowStatsCache, len(missIDs))
		if followRepo == nil {
			for _, uid := range missIDs {
				res[followStatsKey(uid)] = model.FollowStatsCache{}
			}
			return res, nil
		}
		found, err := followRepo.FindStatsByUserIDs(loadCtx, missIDs)
		if err != nil {
			return nil, err
		}
		for _, uid := range missIDs {
			res[followStatsKey(uid)] = found[uid]
		}
		return res, nil
	})
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	for k, v := range cached {
		if uid, ok := keyToID[k]; ok {
			out[uid] = v
		}
	}
	return out, nil
}

func getFollowingIDs(
	ctx context.Context,
	followingCache *cache.MultiLevel[[]uint64],
	followRepo *repository.FollowRepository,
	userID uint64,
) ([]uint64, error) {
	if userID == 0 {
		return []uint64{}, nil
	}
	if followingCache == nil {
		if followRepo == nil {
			return []uint64{}, nil
		}
		ids, err := followRepo.ListAllFollowingIDs(ctx, userID)
		if err != nil {
			return nil, errno.ErrInternal.Wrap(err)
		}
		return ids, nil
	}

	ids, err := followingCache.Get(ctx, followingIDsKey(userID), func(loadCtx context.Context) ([]uint64, error) {
		if followRepo == nil {
			return []uint64{}, nil
		}
		return followRepo.ListAllFollowingIDs(loadCtx, userID)
	})
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}
	return ids, nil
}

func containsSortedID(sortedIDs []uint64, target uint64) bool {
	if len(sortedIDs) == 0 || target == 0 {
		return false
	}
	_, ok := slices.BinarySearch(sortedIDs, target)
	return ok
}
