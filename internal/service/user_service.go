package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/storage"
)

const (
	userLockStripes      = 256
	UserPostsPageDefault = 20
	UserPostsPageMax     = 50
)

type PatchProfileReq struct {
	Nickname  OptionalString `json:"nickname"`
	Bio       OptionalString `json:"bio"`
	AvatarKey OptionalString `json:"avatar_key"`
}

// PostBatchReader 抽象帖子详情批量读取能力（由 PostService 实现）。
type PostBatchReader interface {
	GetPosts(ctx context.Context, ids []uint64) ([]*model.PostDTO, error)
}

// FollowStatsReader 抽象关注计数与关注状态读取能力（由 FollowService 实现，未注入时保持零值）。
type FollowStatsReader interface {
	GetStats(ctx context.Context, userID uint64) (model.FollowStats, error)
	IsFollowing(ctx context.Context, followerID, followeeID uint64) (bool, error)
}

type UserService struct {
	users        UserStore
	posts        PostStore
	postReader   PostBatchReader
	followReader FollowStatsReader
	storage      storage.Service
	profileCache *cache.MultiLevel
	userCfg      config.UserConfig
	locks        [userLockStripes]sync.Mutex
	now          func() time.Time
}

func NewUserService(
	users UserStore,
	posts PostStore,
	postReader PostBatchReader,
	st storage.Service,
	profileCache *cache.MultiLevel,
	userCfg config.UserConfig,
) *UserService {
	return &UserService{
		users:        users,
		posts:        posts,
		postReader:   postReader,
		storage:      st,
		profileCache: profileCache,
		userCfg:      userCfg,
		now:          time.Now,
	}
}

func (s *UserService) userLock(userID uint64) *sync.Mutex {
	return &s.locks[userID%userLockStripes]
}

// SetFollowReader 注入关注计数与状态读取器（由装配层在构造 FollowService 后调用）。
func (s *UserService) SetFollowReader(r FollowStatsReader) {
	s.followReader = r
}

// InvalidateProfile 在 MySQL 写提交后清理指定用户的 L1/L2 资料缓存（按用户 ID 串行化）。
func (s *UserService) InvalidateProfile(ctx context.Context, userID uint64) {
	mu := s.userLock(userID)
	mu.Lock()
	defer mu.Unlock()

	if err := s.profileCache.Delete(ctx, userID); err != nil {
		slog.WarnContext(ctx, "delete user profile l2 cache failed", "user_id", userID, "err", err)
	}
}

func (s *UserService) loadProfileCache(ctx context.Context, missing []uint64) (map[uint64][]byte, error) {
	users, err := s.users.GetPublicByIDs(ctx, missing)
	if err != nil {
		return nil, err
	}
	out := make(map[uint64][]byte, len(users))
	for _, u := range users {
		b, err := json.Marshal(model.NewUserProfileCache(u))
		if err != nil {
			return nil, fmt.Errorf("marshal user profile cache: %w", err)
		}
		out[u.ID] = b
	}
	return out, nil
}

func (s *UserService) profileRedisTTL(uint64) time.Duration {
	return time.Duration(s.userCfg.ProfileRedisTTL) * time.Second
}

func (s *UserService) getProfileCache(ctx context.Context, userID uint64) (*model.UserProfileCache, error) {
	mu := s.userLock(userID)
	mu.Lock()
	defer mu.Unlock()

	vals, err := s.profileCache.GetMany(ctx, []uint64{userID}, s.loadProfileCache, s.profileRedisTTL)
	if err != nil {
		return nil, err
	}
	raw, ok := vals[userID]
	if !ok {
		return nil, errno.ErrUserNotFound
	}
	var pc model.UserProfileCache
	if err := json.Unmarshal(raw, &pc); err != nil {
		slog.WarnContext(ctx, "bad user profile cache", "user_id", userID, "err", err)
		return nil, fmt.Errorf("unmarshal user profile cache: %w", err)
	}
	return &pc, nil
}

// Exists 复用用户资料三级缓存判断用户是否存在（含空值缓存防穿透）。
func (s *UserService) Exists(ctx context.Context, userID uint64) (bool, error) {
	if userID == 0 {
		return false, nil
	}
	_, err := s.getProfileCache(ctx, userID)
	if errors.Is(err, errno.ErrUserNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetPublicProfiles 批量通过三级缓存读取用户公开资料（不含关注计数），供关注/粉丝列表组装。
func (s *UserService) GetPublicProfiles(ctx context.Context, ids []uint64) (map[uint64]*model.UserProfileDTO, error) {
	out := make(map[uint64]*model.UserProfileDTO, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	vals, err := s.profileCache.GetMany(ctx, ids, s.loadProfileCache, s.profileRedisTTL)
	if err != nil {
		return nil, err
	}
	for id, raw := range vals {
		var pc model.UserProfileCache
		if err := json.Unmarshal(raw, &pc); err != nil {
			slog.WarnContext(ctx, "bad user profile cache", "user_id", id, "err", err)
			return nil, fmt.Errorf("unmarshal user profile cache: %w", err)
		}
		out[id] = s.cacheToDTO(&pc)
	}
	return out, nil
}

// GetProfile 通过三级缓存读取用户公开资料并回填关注计数与关注状态。
func (s *UserService) GetProfile(ctx context.Context, userID uint64, viewerID uint64) (*model.UserProfileDTO, error) {
	if userID == 0 {
		return nil, errno.ErrParam
	}
	pc, err := s.getProfileCache(ctx, userID)
	if err != nil {
		return nil, err
	}
	dto := s.cacheToDTO(pc)
	if err := s.fillFollowState(ctx, dto, viewerID); err != nil {
		return nil, err
	}
	return dto, nil
}

func (s *UserService) fillFollowState(ctx context.Context, dto *model.UserProfileDTO, viewerID uint64) error {
	if s.followReader == nil {
		return nil
	}
	stats, err := s.followReader.GetStats(ctx, dto.ID)
	if err != nil {
		return err
	}
	dto.FollowingCount = stats.FollowingCount
	dto.FollowerCount = stats.FollowerCount
	if viewerID > 0 && viewerID != dto.ID {
		following, err := s.followReader.IsFollowing(ctx, viewerID, dto.ID)
		if err != nil {
			return err
		}
		dto.IsFollowing = following
	} else {
		dto.IsFollowing = false
	}
	return nil
}

// UpdateProfile 执行部分字段更新：校验 -> 字段级 UPDATE -> 按主键回读 -> 清理 L1/L2 缓存。
func (s *UserService) UpdateProfile(ctx context.Context, userID uint64, req *PatchProfileReq) (*model.UserProfileDTO, error) {
	if userID == 0 {
		return nil, errno.ErrUnauth
	}
	if !req.Nickname.Set && !req.Bio.Set && !req.AvatarKey.Set {
		return nil, errno.ErrParam.WithMsg("至少提供一个需要更新的字段")
	}

	updates := make(map[string]any, 4)

	if req.Nickname.Set {
		if req.Nickname.Null {
			return nil, errno.ErrParam.WithMsg("昵称不能为 null")
		}
		if err := validateNickname(req.Nickname.Value); err != nil {
			return nil, err
		}
		updates["nickname"] = req.Nickname.Value
	}

	if req.Bio.Set {
		if req.Bio.Null {
			return nil, errno.ErrParam.WithMsg("简介不能为 null")
		}
		if !utf8.ValidString(req.Bio.Value) || utf8.RuneCountInString(req.Bio.Value) > maxBioRunes {
			return nil, errno.ErrParam.WithMsg("简介不能超过 200 个字符")
		}
		updates["bio"] = req.Bio.Value
	}

	if req.AvatarKey.Set {
		if req.AvatarKey.Null {
			return nil, errno.ErrParam.WithMsg("头像 Key 不能为 null")
		}
		key := req.AvatarKey.Value
		if key != "" {
			if !storage.ValidateAvatarKey(userID, key) {
				return nil, errno.ErrParam.WithMsg("头像 Key 无效")
			}
			info, err := s.storage.Stat(ctx, key)
			if errors.Is(err, storage.ErrObjectNotFound) {
				return nil, errno.ErrParam.WithMsg("头像文件不存在，请重新上传")
			}
			if err != nil {
				return nil, err
			}
			if !slices.Contains(storage.AllowedMimes, info.MimeType) || info.Size < 1 || info.Size > storage.AvatarMaxSizeBytes {
				return nil, errno.ErrParam.WithMsg("头像文件类型或大小不符合要求")
			}
		}
		updates["avatar_key"] = key
	}

	updates["updated_at"] = s.now()

	if err := s.users.UpdateFields(ctx, userID, updates); err != nil {
		return nil, err
	}

	// MySQL 提交后立即删缓存，避免后续回读失败时跳过失效导致旧缓存残留；
	// Redis 删除失败只记录日志并已清理本地值，不影响数据库回读返回已提交结果。
	s.InvalidateProfile(ctx, userID)

	pub, err := s.users.GetPublicByID(ctx, userID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, errno.ErrUserNotFound
		}
		return nil, err
	}

	dto := s.publicToDTO(pub)
	if err := s.fillFollowState(ctx, dto, userID); err != nil {
		return nil, err
	}
	return dto, nil
}

// ListUserPosts 按游标分页返回指定用户的正常帖子列表（遵循 OpenAPI 契约：默认 20，允许 1~50）。
func (s *UserService) ListUserPosts(ctx context.Context, targetUserID uint64, rawCursor string, limit int) (*model.FeedPage, error) {
	if targetUserID == 0 {
		return nil, errno.ErrParam
	}
	if limit == 0 {
		limit = UserPostsPageDefault
	}
	if limit < 1 || limit > UserPostsPageMax {
		return nil, errno.ErrParam
	}

	cursorID, err := cursor.DecodeID(rawCursor)
	if err != nil {
		return nil, errno.ErrBadCursor
	}

	// 确认目标用户存在（复用资料缓存）；不存在返回 20003。
	exists, err := s.Exists(ctx, targetUserID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errno.ErrUserNotFound
	}

	ids, err := s.posts.ListUserIDsBefore(ctx, targetUserID, cursorID, limit+1)
	if err != nil {
		return nil, err
	}

	hasMore := len(ids) > limit
	if hasMore {
		ids = ids[:limit]
	}

	nextCursor := ""
	if hasMore && len(ids) > 0 {
		// 游标取本次扫描的最后一个 ID，即使该帖后续被并发删除也不重复扫描。
		nextCursor = cursor.EncodeID(ids[len(ids)-1])
	}

	items, err := s.postReader.GetPosts(ctx, ids)
	if err != nil {
		return nil, err
	}
	if items == nil {
		items = []*model.PostDTO{}
	}
	return &model.FeedPage{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (s *UserService) avatarURL(key string) string {
	if key == "" {
		key = s.userCfg.DefaultAvatarKey
	}
	return s.storage.URL(key)
}

func (s *UserService) cacheToDTO(pc *model.UserProfileCache) *model.UserProfileDTO {
	return &model.UserProfileDTO{
		ID:             pc.ID,
		Username:       pc.Username,
		Nickname:       pc.Nickname,
		AvatarURL:      s.avatarURL(pc.AvatarKey),
		Bio:            pc.Bio,
		FollowingCount: 0,
		FollowerCount:  0,
		IsFollowing:    false,
		CreatedAt:      pc.CreatedAt,
	}
}

func (s *UserService) publicToDTO(u *model.UserPublic) *model.UserProfileDTO {
	return &model.UserProfileDTO{
		ID:             u.ID,
		Username:       u.Username,
		Nickname:       u.Nickname,
		AvatarURL:      s.avatarURL(u.AvatarKey),
		Bio:            u.Bio,
		FollowingCount: 0,
		FollowerCount:  0,
		IsFollowing:    false,
		CreatedAt:      u.CreatedAt.UnixMilli(),
	}
}
