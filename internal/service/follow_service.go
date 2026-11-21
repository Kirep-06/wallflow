package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
)

const (
	followLockStripes     = 256
	FollowListPageDefault = 20
	FollowListPageMax     = 50
)

// UserProfileReader 抽象用户存在性校验与公开资料批量读取能力（由 UserService 实现）。
type UserProfileReader interface {
	Exists(ctx context.Context, userID uint64) (bool, error)
	GetPublicProfiles(ctx context.Context, ids []uint64) (map[uint64]*model.UserProfileDTO, error)
}

type FollowService struct {
	follows    FollowStore
	users      UserProfileReader
	statsCache *cache.MultiLevel
	cfg        config.FollowConfig
	locks      [followLockStripes]sync.Mutex
	now        func() time.Time
}

func NewFollowService(
	follows FollowStore,
	users UserProfileReader,
	statsCache *cache.MultiLevel,
	cfg config.FollowConfig,
) *FollowService {
	return &FollowService{
		follows:    follows,
		users:      users,
		statsCache: statsCache,
		cfg:        cfg,
		now:        time.Now,
	}
}

func (s *FollowService) statsLock(userID uint64) *sync.Mutex {
	return &s.locks[userID%followLockStripes]
}

// InvalidateStats 在 MySQL 写提交后清理单用户的 L1/L2 计数缓存（按用户 ID 串行化，避免与并发读交错回填旧值）。
func (s *FollowService) InvalidateStats(ctx context.Context, userID uint64) {
	if userID == 0 {
		return
	}
	mu := s.statsLock(userID)
	mu.Lock()
	defer mu.Unlock()

	if err := s.statsCache.Delete(ctx, userID); err != nil {
		slog.WarnContext(ctx, "delete follow stats l2 cache failed", "user_id", userID, "err", err)
	}
}

// GetStats 通过三级缓存读取单用户的关注数与粉丝数；无行或命中空值标记均解释为 0/0。
func (s *FollowService) GetStats(ctx context.Context, userID uint64) (model.FollowStats, error) {
	if userID == 0 {
		return model.FollowStats{}, nil
	}
	mu := s.statsLock(userID)
	mu.Lock()
	defer mu.Unlock()

	load := func(ctx context.Context, missing []uint64) (map[uint64][]byte, error) {
		statsMap, err := s.follows.GetStatsByUserIDs(ctx, missing)
		if err != nil {
			return nil, err
		}
		out := make(map[uint64][]byte, len(statsMap))
		for uid, st := range statsMap {
			b, err := json.Marshal(st)
			if err != nil {
				return nil, fmt.Errorf("marshal follow stats cache: %w", err)
			}
			out[uid] = b
		}
		return out, nil
	}
	redisTTL := time.Duration(s.cfg.StatsRedisTTL) * time.Second
	ttl := func(uint64) time.Duration { return redisTTL }

	vals, err := s.statsCache.GetMany(ctx, []uint64{userID}, load, ttl)
	if err != nil {
		return model.FollowStats{}, err
	}
	raw, ok := vals[userID]
	if !ok {
		return model.FollowStats{}, nil
	}
	var st model.FollowStats
	if err := json.Unmarshal(raw, &st); err != nil {
		slog.WarnContext(ctx, "bad follow stats cache", "user_id", userID, "err", err)
		return model.FollowStats{}, fmt.Errorf("unmarshal follow stats cache: %w", err)
	}
	return st, nil
}

// IsFollowing 检查 followerID 是否已关注 followeeID；匿名或查本人恒返回 false。
func (s *FollowService) IsFollowing(ctx context.Context, followerID, followeeID uint64) (bool, error) {
	if followerID == 0 || followeeID == 0 || followerID == followeeID {
		return false, nil
	}
	return s.follows.Exists(ctx, followerID, followeeID)
}

// Follow 幂等关注目标用户。
func (s *FollowService) Follow(ctx context.Context, followerID, followeeID uint64) error {
	if followerID == 0 {
		return errno.ErrUnauth
	}
	if followeeID == 0 {
		return errno.ErrParam
	}
	if followerID == followeeID {
		return errno.ErrFollowSelf
	}

	exists, err := s.users.Exists(ctx, followeeID)
	if err != nil {
		return err
	}
	if !exists {
		return errno.ErrUserNotFound
	}

	changed, err := s.follows.Follow(ctx, followerID, followeeID, s.cfg.MaxFollowing, s.now())
	if err != nil {
		if errors.Is(err, repository.ErrFollowLimitExceeded) {
			return errno.ErrFollowLimitExceeded
		}
		return err
	}
	if changed {
		// 分别按单用户失效，不同时持有两把条纹锁，杜绝同桶/交叉内存锁死锁。
		s.InvalidateStats(ctx, followerID)
		s.InvalidateStats(ctx, followeeID)
	}
	return nil
}

// Unfollow 幂等取消关注目标用户。
func (s *FollowService) Unfollow(ctx context.Context, followerID, followeeID uint64) error {
	if followerID == 0 {
		return errno.ErrUnauth
	}
	if followeeID == 0 {
		return errno.ErrParam
	}
	if followerID == followeeID {
		return nil
	}

	exists, err := s.users.Exists(ctx, followeeID)
	if err != nil {
		return err
	}
	if !exists {
		return errno.ErrUserNotFound
	}

	changed, err := s.follows.Unfollow(ctx, followerID, followeeID, s.now())
	if err != nil {
		return err
	}
	if changed {
		s.InvalidateStats(ctx, followerID)
		s.InvalidateStats(ctx, followeeID)
	}
	return nil
}

// ListFollowing 返回指定用户的关注列表（游标分页，按关注时间/关系 ID 倒序）。
func (s *FollowService) ListFollowing(ctx context.Context, targetUserID, viewerID uint64, rawCursor string, limit int) (*model.FollowUserPage, error) {
	return s.listUsers(ctx, targetUserID, viewerID, rawCursor, limit, s.follows.ListFollowingBefore)
}

// ListFollowers 返回指定用户的粉丝列表（游标分页，按关注时间/关系 ID 倒序）。
func (s *FollowService) ListFollowers(ctx context.Context, targetUserID, viewerID uint64, rawCursor string, limit int) (*model.FollowUserPage, error) {
	return s.listUsers(ctx, targetUserID, viewerID, rawCursor, limit, s.follows.ListFollowersBefore)
}

func (s *FollowService) listUsers(
	ctx context.Context,
	targetUserID, viewerID uint64,
	rawCursor string,
	limit int,
	listEdges func(ctx context.Context, uid, beforeID uint64, limit int) ([]model.FollowEdge, error),
) (*model.FollowUserPage, error) {
	if targetUserID == 0 {
		return nil, errno.ErrParam
	}
	if limit == 0 {
		limit = FollowListPageDefault
	}
	if limit < 1 || limit > FollowListPageMax {
		return nil, errno.ErrParam
	}

	beforeID, err := cursor.DecodeID(rawCursor)
	if err != nil {
		return nil, errno.ErrBadCursor
	}

	exists, err := s.users.Exists(ctx, targetUserID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errno.ErrUserNotFound
	}

	edges, err := listEdges(ctx, targetUserID, beforeID, limit+1)
	if err != nil {
		return nil, err
	}

	hasMore := len(edges) > limit
	if hasMore {
		edges = edges[:limit]
	}

	nextCursor := ""
	if hasMore && len(edges) > 0 {
		nextCursor = cursor.EncodeID(edges[len(edges)-1].ID)
	}
	if len(edges) == 0 {
		return &model.FollowUserPage{
			Items:      []*model.FollowUserDTO{},
			NextCursor: "",
			HasMore:    false,
		}, nil
	}

	targetIDs := make([]uint64, 0, len(edges))
	checkIDs := make([]uint64, 0, len(edges))
	for _, e := range edges {
		targetIDs = append(targetIDs, e.UserID)
		if viewerID > 0 && e.UserID != viewerID {
			checkIDs = append(checkIDs, e.UserID)
		}
	}

	profiles, err := s.users.GetPublicProfiles(ctx, targetIDs)
	if err != nil {
		return nil, err
	}

	var followingMap map[uint64]bool
	if viewerID > 0 && len(checkIDs) > 0 {
		followingMap, err = s.follows.BatchIsFollowing(ctx, viewerID, checkIDs)
		if err != nil {
			return nil, err
		}
	}

	items := make([]*model.FollowUserDTO, 0, len(edges))
	for _, e := range edges {
		p, ok := profiles[e.UserID]
		if !ok {
			continue
		}
		items = append(items, &model.FollowUserDTO{
			ID:          p.ID,
			Username:    p.Username,
			Nickname:    p.Nickname,
			AvatarURL:   p.AvatarURL,
			Bio:         p.Bio,
			IsFollowing: viewerID > 0 && p.ID != viewerID && followingMap[p.ID],
		})
	}

	return &model.FollowUserPage{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}
