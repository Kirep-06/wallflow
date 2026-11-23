package service

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"unicode/utf8"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/storage"
)

const (
	userProfileKeyPrefix = "user:profile:"
	defaultAvatarKey     = "avatars/default.png"
	maxBioRunes          = 200
)

// UserServiceDeps 聚合用户领域服务所需依赖，保持与下层仓储/缓存的单向无环装配。
type UserServiceDeps struct {
	UserRepo          *repository.UserRepository
	WallpaperRepo     *repository.WallpaperRepository
	FollowRepo        *repository.FollowRepository
	ProfileCache      *cache.MultiLevel[model.UserProfileCache]
	FollowStatsCache  *cache.MultiLevel[model.FollowStatsCache]
	FollowingIDsCache *cache.MultiLevel[[]uint64]
	Store             storage.StorageService
}

// UserService 负责用户资料三级缓存读取、字段级更新、创作者公开主页与作者信息组装。
type UserService struct {
	userRepo          *repository.UserRepository
	wallpaperRepo     *repository.WallpaperRepository
	followRepo        *repository.FollowRepository
	profileCache      *cache.MultiLevel[model.UserProfileCache]
	followStatsCache  *cache.MultiLevel[model.FollowStatsCache]
	followingIDsCache *cache.MultiLevel[[]uint64]
	store             storage.StorageService
}

// NewUserService 构造用户领域服务。
func NewUserService(deps UserServiceDeps) *UserService {
	return &UserService{
		userRepo:          deps.UserRepo,
		wallpaperRepo:     deps.WallpaperRepo,
		followRepo:        deps.FollowRepo,
		profileCache:      deps.ProfileCache,
		followStatsCache:  deps.FollowStatsCache,
		followingIDsCache: deps.FollowingIDsCache,
		store:             deps.Store,
	}
}

// GetMe 获取当前登录用户完整资料。
func (s *UserService) GetMe(ctx context.Context, userID uint64) (*model.UserProfile, error) {
	cached, err := s.getCachedProfile(ctx, userID)
	if err != nil {
		if errors.Is(err, cache.ErrNotFound) {
			return nil, errno.ErrUnauthorized
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	profile, err := s.BuildUserProfile(ctx, cached)
	if err != nil {
		return nil, err
	}
	return &profile, nil
}

// PatchMe 执行字段级资料更新：校验参数 -> 头像远程核验 -> 写 MySQL -> 先删 L1 再删 L2 -> 主键回读 MySQL。
func (s *UserService) PatchMe(ctx context.Context, userID uint64, req model.PatchProfileRequest) (*model.UserProfile, error) {
	updates, avatarKey, err := validatePatchProfile(userID, req)
	if err != nil {
		return nil, err
	}

	if avatarKey != "" {
		if err := verifyAvatarObject(ctx, s.store, userID, avatarKey); err != nil {
			return nil, err
		}
	}

	if err := s.userRepo.UpdateProfileFields(ctx, userID, updates); err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	// 写后失效：先删 L1 再删 L2；Redis 异常记告警不阻断已提交的写请求。
	s.InvalidateProfileCache(ctx, userID)

	fresh, err := s.userRepo.FindProfileByID(ctx, userID)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return nil, errno.ErrUnauthorized
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	profile, err := s.BuildUserProfile(ctx, *fresh)
	if err != nil {
		return nil, err
	}
	return &profile, nil
}

// GetPublicProfile 通过三级缓存查看创作者公开主页（含真实关注/粉丝数与 is_following 状态）。
func (s *UserService) GetPublicProfile(ctx context.Context, targetID, viewerID uint64) (*model.UserPublicProfile, error) {
	cached, err := s.getCachedProfile(ctx, targetID)
	if err != nil {
		if errors.Is(err, cache.ErrNotFound) {
			return nil, errno.ErrUserNotFound
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	profile, err := s.BuildUserProfile(ctx, cached)
	if err != nil {
		return nil, err
	}

	isFollowing := false
	if viewerID > 0 && viewerID != targetID {
		followingIDs, err := getFollowingIDs(ctx, s.followingIDsCache, s.followRepo, viewerID)
		if err != nil {
			return nil, err
		}
		isFollowing = containsSortedID(followingIDs, targetID)
	}

	return &model.UserPublicProfile{
		UserProfile: profile,
		IsFollowing: isFollowing,
	}, nil
}

// EnsureExists 通过用户资料三级缓存校验目标用户是否存在（自动享受空值缓存防穿透）。
func (s *UserService) EnsureExists(ctx context.Context, userID uint64) error {
	if _, err := s.getCachedProfile(ctx, userID); err != nil {
		if errors.Is(err, cache.ErrNotFound) {
			return errno.ErrUserNotFound
		}
		return errno.ErrInternal.Wrap(err)
	}
	return nil
}

// GetAuthor 返回壁纸详情内嵌的作者展示信息。
func (s *UserService) GetAuthor(ctx context.Context, userID uint64) (model.WallpaperAuthor, error) {
	cached, err := s.getCachedProfile(ctx, userID)
	if err != nil {
		if errors.Is(err, cache.ErrNotFound) {
			return model.WallpaperAuthor{}, errno.ErrUserNotFound
		}
		return model.WallpaperAuthor{}, errno.ErrInternal.Wrap(err)
	}
	return s.toWallpaperAuthor(cached), nil
}

// GetProfilesByIDs 通过三级缓存 MGet 批量获取公开用户资料快照，供列表页消除 N+1 查询。
func (s *UserService) GetProfilesByIDs(ctx context.Context, userIDs []uint64) (map[uint64]model.UserProfileCache, error) {
	if len(userIDs) == 0 {
		return map[uint64]model.UserProfileCache{}, nil
	}

	keys := make([]string, 0, len(userIDs))
	keyToID := make(map[string]uint64, len(userIDs))
	for _, uid := range userIDs {
		if uid == 0 {
			continue
		}
		k := userProfileKey(uid)
		if _, dup := keyToID[k]; dup {
			continue
		}
		keyToID[k] = uid
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return map[uint64]model.UserProfileCache{}, nil
	}

	profiles, err := s.profileCache.MGet(ctx, keys, func(loadCtx context.Context, missKeys []string) (map[string]model.UserProfileCache, error) {
		missIDs := make([]uint64, 0, len(missKeys))
		for _, k := range missKeys {
			if uid, ok := keyToID[k]; ok {
				missIDs = append(missIDs, uid)
			}
		}
		found, err := s.userRepo.FindProfilesByIDs(loadCtx, missIDs)
		if err != nil {
			return nil, err
		}
		out := make(map[string]model.UserProfileCache, len(found))
		for uid, p := range found {
			out[userProfileKey(uid)] = p
		}
		return out, nil
	})
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	out := make(map[uint64]model.UserProfileCache, len(profiles))
	for _, p := range profiles {
		out[p.ID] = p
	}
	return out, nil
}

// GetAuthorsByIDs 通过三级缓存 MGet 批量获取作者展示信息，用于壁纸列表页消除 N+1 查询。
func (s *UserService) GetAuthorsByIDs(ctx context.Context, userIDs []uint64) (map[uint64]model.WallpaperAuthor, error) {
	profiles, err := s.GetProfilesByIDs(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	authors := make(map[uint64]model.WallpaperAuthor, len(profiles))
	for uid, p := range profiles {
		authors[uid] = s.toWallpaperAuthor(p)
	}
	return authors, nil
}

func (s *UserService) toWallpaperAuthor(p model.UserProfileCache) model.WallpaperAuthor {
	return model.WallpaperAuthor{
		ID:        p.ID,
		Username:  p.Username,
		Nickname:  p.Nickname,
		AvatarURL: s.BuildAvatarURL(p.AvatarKey),
	}
}

// InvalidateProfileCache 失效指定用户的 L1 + L2 资料缓存。
func (s *UserService) InvalidateProfileCache(ctx context.Context, userID uint64) {
	if s.profileCache == nil || userID == 0 {
		return
	}
	if err := s.profileCache.Delete(ctx, userProfileKey(userID)); err != nil {
		slog.WarnContext(ctx, "invalidate user profile cache failed",
			slog.Uint64("user_id", userID),
			slog.Any("err", err),
		)
	}
}

// BuildUserProfile 查询用户已发布壁纸总数与关注统计，组装完整 UserProfile DTO。
func (s *UserService) BuildUserProfile(ctx context.Context, p model.UserProfileCache) (model.UserProfile, error) {
	var wpCount int64
	if s.wallpaperRepo != nil {
		var err error
		wpCount, err = s.wallpaperRepo.CountActiveByUserID(ctx, p.ID)
		if err != nil {
			return model.UserProfile{}, errno.ErrInternal.Wrap(err)
		}
	}

	stats, err := getFollowStats(ctx, s.followStatsCache, s.followRepo, p.ID)
	if err != nil {
		return model.UserProfile{}, err
	}
	return s.ToUserProfile(p, wpCount, stats), nil
}

// ToUserProfile 将缓存资料结构、作品数与关注统计转换为对外 HTTP DTO。
func (s *UserService) ToUserProfile(p model.UserProfileCache, wallpaperCount int64, stats model.FollowStatsCache) model.UserProfile {
	return model.UserProfile{
		ID:             p.ID,
		Username:       p.Username,
		Nickname:       p.Nickname,
		AvatarURL:      s.BuildAvatarURL(p.AvatarKey),
		Bio:            p.Bio,
		FollowingCount: stats.FollowingCount,
		FollowerCount:  stats.FollowerCount,
		WallpaperCount: wallpaperCount,
		CreatedAt:      p.CreatedAt.UTC(),
	}
}

// BuildAvatarURL 根据对象存储配置与默认头像 key 组装可直接渲染的头像地址。
func (s *UserService) BuildAvatarURL(avatarKey string) string {
	if s.store == nil {
		return ""
	}
	key := cmp.Or(strings.TrimLeft(strings.TrimSpace(avatarKey), "/"), defaultAvatarKey)
	return s.store.BuildRawURL(key)
}

func (s *UserService) getCachedProfile(ctx context.Context, userID uint64) (model.UserProfileCache, error) {
	return s.profileCache.Get(ctx, userProfileKey(userID), func(loadCtx context.Context) (model.UserProfileCache, error) {
		p, err := s.userRepo.FindProfileByID(loadCtx, userID)
		if err != nil {
			if errors.Is(err, repository.ErrUserNotFound) {
				return model.UserProfileCache{}, cache.ErrNotFound
			}
			return model.UserProfileCache{}, err
		}
		return *p, nil
	})
}

func validatePatchProfile(userID uint64, req model.PatchProfileRequest) (map[string]any, string, error) {
	updates := make(map[string]any, 3)
	var verifyKey string

	if req.Nickname.Set {
		if req.Nickname.Null {
			return nil, "", errno.ErrInvalidParam.WithMessage("nickname 不能为 null")
		}
		nick, err := validateNickname(req.Nickname.Value)
		if err != nil {
			return nil, "", err
		}
		updates["nickname"] = nick
	}

	if req.Bio.Set {
		if req.Bio.Null {
			return nil, "", errno.ErrInvalidParam.WithMessage("bio 不能为 null")
		}
		if utf8.RuneCountInString(req.Bio.Value) > maxBioRunes {
			return nil, "", errno.ErrInvalidParam.WithMessage("个人简介长度不能超过 200 个字符")
		}
		updates["bio"] = req.Bio.Value
	}

	if req.AvatarKey.Set {
		if req.AvatarKey.Null {
			return nil, "", errno.ErrInvalidParam.WithMessage("avatar_key 不能为 null")
		}
		if req.AvatarKey.Value != "" && !storage.ValidateObjectKey(storage.SceneAvatar, userID, req.AvatarKey.Value) {
			return nil, "", errno.ErrInvalidParam.WithMessage("头像路径不合法")
		}
		updates["avatar_key"] = req.AvatarKey.Value
		verifyKey = req.AvatarKey.Value
	}

	if len(updates) == 0 {
		return nil, "", errno.ErrInvalidParam.WithMessage("至少提供一个有效更新字段")
	}
	return updates, verifyKey, nil
}

func userProfileKey(userID uint64) string {
	return userProfileKeyPrefix + strconv.FormatUint(userID, 10)
}
