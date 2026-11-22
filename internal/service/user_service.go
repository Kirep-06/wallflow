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

// UserService 负责用户资料三级缓存读取、字段级更新、创作者公开主页与作者信息组装。
type UserService struct {
	userRepo      *repository.UserRepository
	wallpaperRepo *repository.WallpaperRepository
	profileCache  *cache.MultiLevel[model.UserProfileCache]
	store         storage.StorageService
}

// NewUserService 构造用户领域服务。
func NewUserService(
	userRepo *repository.UserRepository,
	wallpaperRepo *repository.WallpaperRepository,
	profileCache *cache.MultiLevel[model.UserProfileCache],
	store storage.StorageService,
) *UserService {
	return &UserService{
		userRepo:      userRepo,
		wallpaperRepo: wallpaperRepo,
		profileCache:  profileCache,
		store:         store,
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

// GetPublicProfile 通过三级缓存查看创作者公开主页（关注计数与 is_following 留待 M4 接入）。
func (s *UserService) GetPublicProfile(ctx context.Context, targetID uint64, _ uint64) (*model.UserPublicProfile, error) {
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
	return &model.UserPublicProfile{
		UserProfile: profile,
		IsFollowing: false,
	}, nil
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
	return model.WallpaperAuthor{
		ID:        cached.ID,
		Username:  cached.Username,
		Nickname:  cached.Nickname,
		AvatarURL: s.BuildAvatarURL(cached.AvatarKey),
	}, nil
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

// BuildUserProfile 直查用户已发布壁纸总数并组装完整 UserProfile DTO。
func (s *UserService) BuildUserProfile(ctx context.Context, p model.UserProfileCache) (model.UserProfile, error) {
	count, err := s.wallpaperRepo.CountActiveByUserID(ctx, p.ID)
	if err != nil {
		return model.UserProfile{}, errno.ErrInternal.Wrap(err)
	}
	return s.ToUserProfile(p, count), nil
}

// ToUserProfile 将缓存资料结构与作品数转换为对外 HTTP DTO。
func (s *UserService) ToUserProfile(p model.UserProfileCache, wallpaperCount int64) model.UserProfile {
	return model.UserProfile{
		ID:             p.ID,
		Username:       p.Username,
		Nickname:       p.Nickname,
		AvatarURL:      s.BuildAvatarURL(p.AvatarKey),
		Bio:            p.Bio,
		FollowingCount: 0,
		FollowerCount:  0,
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
