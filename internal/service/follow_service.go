package service

import (
	"context"
	"errors"
	"log/slog"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/repository"
)

const (
	// DefaultMaxFollowingCount 为单用户默认关注数硬上限（防 Redis BigKey 与 M5 读扩散爆炸）。
	DefaultMaxFollowingCount int64 = 2000
	// DefaultBigVPromoteThreshold 为大 V 单向原子晋升默认粉丝阈值。
	DefaultBigVPromoteThreshold int64 = 1000
)

// FollowOption 配置 FollowService 的可选业务阈值（便于测试注入小阈值）。
type FollowOption func(*FollowService)

// WithMaxFollowingCount 覆盖最大关注数上限（<=0 时保持默认值）。
func WithMaxFollowingCount(n int64) FollowOption {
	return func(s *FollowService) {
		if n > 0 {
			s.maxFollowing = n
		}
	}
}

// WithBigVPromoteThreshold 覆盖大 V 原子晋升粉丝阈值（<=0 时保持默认值）。
func WithBigVPromoteThreshold(n int64) FollowOption {
	return func(s *FollowService) {
		if n > 0 {
			s.bigVThreshold = n
		}
	}
}

// FollowService 负责关注/取关写事务编排、缓存失效与关注/粉丝列表批量装配。
type FollowService struct {
	followRepo        *repository.FollowRepository
	userSvc           *UserService
	followStatsCache  *cache.MultiLevel[model.FollowStatsCache]
	followingIDsCache *cache.MultiLevel[[]uint64]
	maxFollowing      int64
	bigVThreshold     int64
}

// NewFollowService 构造关注关系领域服务。
func NewFollowService(
	followRepo *repository.FollowRepository,
	userSvc *UserService,
	followStatsCache *cache.MultiLevel[model.FollowStatsCache],
	followingIDsCache *cache.MultiLevel[[]uint64],
	opts ...FollowOption,
) *FollowService {
	s := &FollowService{
		followRepo:        followRepo,
		userSvc:           userSvc,
		followStatsCache:  followStatsCache,
		followingIDsCache: followingIDsCache,
		maxFollowing:      DefaultMaxFollowingCount,
		bigVThreshold:     DefaultBigVPromoteThreshold,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// Follow 关注指定创作者（防自关、校验目标存在、幂等优先于上限、防死锁事务 + 写后失效缓存）。
func (s *FollowService) Follow(ctx context.Context, followerID, targetID uint64) (*model.FollowActionResult, error) {
	if followerID == targetID {
		return nil, errno.ErrCannotFollowSelf
	}
	if err := s.userSvc.EnsureExists(ctx, targetID); err != nil {
		return nil, err
	}

	res, err := s.followRepo.Follow(ctx, followerID, targetID, s.maxFollowing, s.bigVThreshold)
	if err != nil {
		if errors.Is(err, repository.ErrFollowLimitReached) {
			return nil, errno.ErrFollowLimitReached
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	if res.Changed {
		s.invalidateAfterChange(ctx, followerID, targetID)
	}

	return &model.FollowActionResult{
		IsFollowing:   true,
		FollowerCount: res.FollowerCount,
	}, nil
}

// Unfollow 取消关注指定创作者（取关本人或未关注均严格幂等返回 200）。
func (s *FollowService) Unfollow(ctx context.Context, followerID, targetID uint64) (*model.FollowActionResult, error) {
	if followerID == targetID {
		stats, err := getFollowStats(ctx, s.followStatsCache, s.followRepo, targetID)
		if err != nil {
			return nil, err
		}
		return &model.FollowActionResult{
			IsFollowing:   false,
			FollowerCount: stats.FollowerCount,
		}, nil
	}
	if err := s.userSvc.EnsureExists(ctx, targetID); err != nil {
		return nil, err
	}

	res, err := s.followRepo.Unfollow(ctx, followerID, targetID)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	if res.Changed {
		s.invalidateAfterChange(ctx, followerID, targetID)
	}

	return &model.FollowActionResult{
		IsFollowing:   false,
		FollowerCount: res.FollowerCount,
	}, nil
}

// ListFollowing 游标分页查询指定用户的关注列表（覆盖索引零回表 + 批量三级缓存装配）。
func (s *FollowService) ListFollowing(
	ctx context.Context,
	targetUserID, viewerID uint64,
	rawCursor string,
	rawLimit int,
) (*response.PageData[model.FollowUserItem], error) {
	return s.listFollowUsers(ctx, targetUserID, viewerID, rawCursor, rawLimit, s.followRepo.ListFollowingPage)
}

// ListFollowers 游标分页查询指定用户的粉丝列表（覆盖索引零回表 + 批量三级缓存装配）。
func (s *FollowService) ListFollowers(
	ctx context.Context,
	targetUserID, viewerID uint64,
	rawCursor string,
	rawLimit int,
) (*response.PageData[model.FollowUserItem], error) {
	return s.listFollowUsers(ctx, targetUserID, viewerID, rawCursor, rawLimit, s.followRepo.ListFollowerPage)
}

func (s *FollowService) listFollowUsers(
	ctx context.Context,
	targetUserID, viewerID uint64,
	rawCursor string,
	rawLimit int,
	fetchEdges func(context.Context, uint64, uint64, int) ([]model.FollowEdge, error),
) (*response.PageData[model.FollowUserItem], error) {
	limit, err := normalizeListLimit(rawLimit)
	if err != nil {
		return nil, err
	}
	beforeID, err := parseIDCursor(rawCursor)
	if err != nil {
		return nil, err
	}

	if err := s.userSvc.EnsureExists(ctx, targetUserID); err != nil {
		return nil, err
	}

	edges, err := fetchEdges(ctx, targetUserID, beforeID, limit+1)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	pageEdges, nextCursor, hasMore, err := paginateFollowEdges(edges, limit)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}
	return s.assembleFollowUserPage(ctx, pageEdges, viewerID, nextCursor, hasMore)
}

func (s *FollowService) assembleFollowUserPage(
	ctx context.Context,
	edges []model.FollowEdge,
	viewerID uint64,
	nextCursor string,
	hasMore bool,
) (*response.PageData[model.FollowUserItem], error) {
	if len(edges) == 0 {
		page := response.NewPageData[model.FollowUserItem](nil, nextCursor, hasMore)
		return &page, nil
	}

	peerIDs := make([]uint64, 0, len(edges))
	for _, e := range edges {
		peerIDs = append(peerIDs, e.PeerID)
	}

	profiles, err := s.userSvc.GetProfilesByIDs(ctx, peerIDs)
	if err != nil {
		return nil, err
	}
	statsMap, err := mgetFollowStats(ctx, s.followStatsCache, s.followRepo, peerIDs)
	if err != nil {
		return nil, err
	}

	var viewerFollowing []uint64
	if viewerID > 0 {
		viewerFollowing, err = getFollowingIDs(ctx, s.followingIDsCache, s.followRepo, viewerID)
		if err != nil {
			return nil, err
		}
	}

	items := make([]model.FollowUserItem, 0, len(edges))
	for _, e := range edges {
		p, ok := profiles[e.PeerID]
		if !ok {
			continue
		}
		items = append(items, model.FollowUserItem{
			ID:            p.ID,
			Username:      p.Username,
			Nickname:      p.Nickname,
			AvatarURL:     s.userSvc.BuildAvatarURL(p.AvatarKey),
			Bio:           p.Bio,
			FollowerCount: statsMap[e.PeerID].FollowerCount,
			IsFollowing:   viewerID > 0 && e.PeerID != viewerID && containsSortedID(viewerFollowing, e.PeerID),
		})
	}

	page := response.NewPageData(items, nextCursor, hasMore)
	return &page, nil
}

func (s *FollowService) invalidateAfterChange(ctx context.Context, followerID, followeeID uint64) {
	if s.followStatsCache != nil {
		for _, uid := range []uint64{followerID, followeeID} {
			if err := s.followStatsCache.Delete(ctx, followStatsKey(uid)); err != nil {
				slog.WarnContext(ctx, "invalidate follow stats cache failed",
					slog.Uint64("user_id", uid),
					slog.Any("err", err),
				)
			}
		}
	}
	if s.followingIDsCache != nil {
		if err := s.followingIDsCache.Delete(ctx, followingIDsKey(followerID)); err != nil {
			slog.WarnContext(ctx, "invalidate following ids cache failed",
				slog.Uint64("user_id", followerID),
				slog.Any("err", err),
			)
		}
	}
}

func paginateFollowEdges(raw []model.FollowEdge, limit int) (page []model.FollowEdge, nextCursor string, hasMore bool, err error) {
	if len(raw) <= limit {
		return raw, "", false, nil
	}
	page = raw[:limit]
	nextCursor, err = cursor.Encode(cursor.IDCursor{ID: page[len(page)-1].ID})
	if err != nil {
		return nil, "", false, err
	}
	return page, nextCursor, true, nil
}
