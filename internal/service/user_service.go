package service

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
)

const (
	userProfileKeyPrefix = "user:profile:"
	maxNicknameRunes     = 32
	maxBioRunes          = 200
	maxAvatarKeyLen      = 255
)

var avatarKeySuffixPattern = regexp.MustCompile(`^[a-zA-Z0-9._/-]+$`)

// UserService 负责用户资料三级缓存读取、字段级更新与创作者公开主页组装。
type UserService struct {
	userRepo         repository.UserRepository
	profileCache     *cache.MultiLevel[model.UserProfileCache]
	cdnDomain        string
	defaultAvatarKey string
	logger           *slog.Logger
}

// NewUserService 构造用户领域服务。
func NewUserService(
	userRepo repository.UserRepository,
	profileCache *cache.MultiLevel[model.UserProfileCache],
	cdnDomain string,
	defaultAvatarKey string,
	logger *slog.Logger,
) *UserService {
	if logger == nil {
		logger = slog.Default()
	}
	return &UserService{
		userRepo:         userRepo,
		profileCache:     profileCache,
		cdnDomain:        strings.TrimRight(strings.TrimSpace(cdnDomain), "/"),
		defaultAvatarKey: strings.TrimLeft(strings.TrimSpace(defaultAvatarKey), "/"),
		logger:           logger,
	}
}

// GetMe 获取当前登录用户完整资料，并实时校验账号封禁状态。
func (s *UserService) GetMe(ctx context.Context, userID uint64) (*model.UserProfile, error) {
	if err := s.ensureUserActive(ctx, userID); err != nil {
		return nil, err
	}

	cached, err := s.getCachedProfile(ctx, userID)
	if err != nil {
		if errors.Is(err, cache.ErrNotFound) {
			return nil, errno.ErrUnauthorized
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	profile := s.ToUserProfile(cached)
	return &profile, nil
}

// PatchMe 执行字段级资料更新：校验参数 -> 检查账号状态 -> 写 MySQL -> 先删 L1 再删 L2 -> 主键回读 MySQL。
func (s *UserService) PatchMe(ctx context.Context, userID uint64, req model.PatchProfileRequest) (*model.UserProfile, error) {
	updates, err := validatePatchProfile(userID, req)
	if err != nil {
		return nil, err
	}

	if err := s.ensureUserActive(ctx, userID); err != nil {
		return nil, err
	}

	if err := s.userRepo.UpdateProfileFields(ctx, userID, updates); err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	// 写后失效：先删 L1 再删 L2；Redis 异常记告警不阻断已提交的写请求。
	s.InvalidateProfileCache(ctx, userID)

	fresh, err := s.userRepo.FindByID(ctx, userID)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return nil, errno.ErrUnauthorized
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	profile := s.ToUserProfile(fresh.ToProfileCache())
	return &profile, nil
}

// GetPublicProfile 通过三级缓存查看创作者公开主页（M1 计数固定 0、is_following 固定 false）。
func (s *UserService) GetPublicProfile(ctx context.Context, targetID uint64, _ uint64) (*model.UserPublicProfile, error) {
	cached, err := s.getCachedProfile(ctx, targetID)
	if err != nil {
		if errors.Is(err, cache.ErrNotFound) {
			return nil, errno.ErrUserNotFound
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	return &model.UserPublicProfile{
		UserProfile: s.ToUserProfile(cached),
		IsFollowing: false,
	}, nil
}

// InvalidateProfileCache 失效指定用户的 L1 + L2 资料缓存。
func (s *UserService) InvalidateProfileCache(ctx context.Context, userID uint64) {
	if s.profileCache == nil || userID == 0 {
		return
	}
	if err := s.profileCache.Delete(ctx, userProfileKey(userID)); err != nil {
		s.logger.WarnContext(ctx, "invalidate user profile cache failed",
			slog.Uint64("user_id", userID),
			slog.Any("err", err),
		)
	}
}

// ToUserProfile 将缓存资料结构转换为对外 HTTP DTO（所有 ID 序列化为 string）。
func (s *UserService) ToUserProfile(p model.UserProfileCache) model.UserProfile {
	return model.UserProfile{
		ID:             strconv.FormatUint(p.ID, 10),
		Username:       p.Username,
		Nickname:       p.Nickname,
		AvatarURL:      s.BuildAvatarURL(p.AvatarKey),
		Bio:            p.Bio,
		FollowingCount: 0,
		FollowerCount:  0,
		WallpaperCount: 0,
		CreatedAt:      p.CreatedAt.UTC(),
	}
}

// BuildAvatarURL 根据 CDN 域名与默认头像 key 组装可直接渲染的头像地址。
func (s *UserService) BuildAvatarURL(avatarKey string) string {
	if s.cdnDomain == "" {
		return ""
	}
	key := strings.TrimLeft(strings.TrimSpace(avatarKey), "/")
	if key == "" {
		key = s.defaultAvatarKey
	}
	if key == "" {
		return ""
	}
	return s.cdnDomain + "/" + key
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

func (s *UserService) ensureUserActive(ctx context.Context, userID uint64) error {
	status, err := s.userRepo.GetStatusByID(ctx, userID)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return errno.ErrUnauthorized
		}
		return errno.ErrInternal.Wrap(err)
	}
	if status != model.UserStatusActive {
		return errno.ErrAccountBanned
	}
	return nil
}

func validatePatchProfile(userID uint64, req model.PatchProfileRequest) (map[string]any, error) {
	updates := make(map[string]any, 3)

	if req.Nickname.Set {
		if req.Nickname.Null {
			return nil, errno.ErrInvalidParam.WithMessage("nickname 不能为 null")
		}
		nick := strings.TrimSpace(req.Nickname.Value)
		n := utf8.RuneCountInString(nick)
		if n < 1 || n > maxNicknameRunes {
			return nil, errno.ErrInvalidParam.WithMessage("昵称长度需为 1~32 个字符")
		}
		updates["nickname"] = nick
	}

	if req.Bio.Set {
		if req.Bio.Null {
			return nil, errno.ErrInvalidParam.WithMessage("bio 不能为 null")
		}
		if utf8.RuneCountInString(req.Bio.Value) > maxBioRunes {
			return nil, errno.ErrInvalidParam.WithMessage("个人简介长度不能超过 200 个字符")
		}
		updates["bio"] = req.Bio.Value
	}

	if req.AvatarKey.Set {
		if req.AvatarKey.Null {
			return nil, errno.ErrInvalidParam.WithMessage("avatar_key 不能为 null")
		}
		if err := validateAvatarKey(userID, req.AvatarKey.Value); err != nil {
			return nil, err
		}
		updates["avatar_key"] = req.AvatarKey.Value
	}

	if len(updates) == 0 {
		return nil, errno.ErrInvalidParam.WithMessage("至少提供一个有效更新字段")
	}
	return updates, nil
}

func validateAvatarKey(userID uint64, key string) error {
	if key == "" {
		return nil
	}
	if len(key) > maxAvatarKeyLen || strings.Contains(key, "..") {
		return errno.ErrInvalidParam.WithMessage("头像路径不合法")
	}
	prefix := "avatars/" + strconv.FormatUint(userID, 10) + "/"
	if !strings.HasPrefix(key, prefix) {
		return errno.ErrInvalidParam.WithMessage("头像路径不合法")
	}
	suffix := strings.TrimPrefix(key, prefix)
	if !avatarKeySuffixPattern.MatchString(suffix) {
		return errno.ErrInvalidParam.WithMessage("头像路径不合法")
	}
	return nil
}

func userProfileKey(userID uint64) string {
	return userProfileKeyPrefix + strconv.FormatUint(userID, 10)
}
