package service

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/storage"
)

const defaultAdminTagLimit = 200

// AdminMetaService 负责管理后台的鉴权校验与标签增删改业务。
type AdminMetaService struct {
	adminCfg       config.AdminConfig
	userRepo       *repository.UserRepository
	metaRepo       *repository.MetaRepository
	wallpaperCache *cache.MultiLevel[model.WallpaperDetailCache]
	store          storage.StorageService
}

// NewAdminMetaService 构造后台元数据管理服务。
func NewAdminMetaService(
	adminCfg config.AdminConfig,
	userRepo *repository.UserRepository,
	metaRepo *repository.MetaRepository,
	wallpaperCache *cache.MultiLevel[model.WallpaperDetailCache],
	store storage.StorageService,
) *AdminMetaService {
	return &AdminMetaService{
		adminCfg:       adminCfg,
		userRepo:       userRepo,
		metaRepo:       metaRepo,
		wallpaperCache: wallpaperCache,
		store:          store,
	}
}

// RequireAdmin 校验 userID 对应用户名是否在管理员白名单内，通过时返回用户资料快照。
func (s *AdminMetaService) RequireAdmin(ctx context.Context, userID uint64) (*model.UserProfileCache, error) {
	if userID == 0 {
		return nil, errno.ErrUnauthorized
	}
	user, err := s.userRepo.FindProfileByID(ctx, userID)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return nil, errno.ErrUnauthorized
		}
		return nil, errno.ErrInternal.Wrap(err)
	}
	if !s.adminCfg.IsAdminUsername(user.Username) {
		return nil, errno.ErrForbidden.WithMessage("当前账号无管理后台权限")
	}
	return user, nil
}

// ListTags 返回后台标签列表（支持关键词前缀过滤）。
func (s *AdminMetaService) ListTags(ctx context.Context, rawQuery string, limit int) (*model.TagList, error) {
	if limit <= 0 || limit > 500 {
		limit = defaultAdminTagLimit
	}
	q := strings.ToLower(strings.TrimSpace(rawQuery))
	var (
		tags []model.Tag
		err  error
	)
	if q == "" {
		tags, err = s.metaRepo.ListHotTags(ctx, limit)
	} else {
		tags, err = s.metaRepo.SearchTagsByPrefix(ctx, escapeLikePrefix(q), limit)
	}
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}
	items := make([]model.TagItem, 0, len(tags))
	for i := range tags {
		items = append(items, tags[i].ToItem(s.store.BuildRawURL(tags[i].IconKey)))
	}
	return &model.TagList{Items: items}, nil
}

// CreateTag 新增标签。
func (s *AdminMetaService) CreateTag(ctx context.Context, adminUserID uint64, req model.AdminCreateTagRequest) (*model.TagItem, error) {
	name := strings.ToLower(strings.TrimSpace(req.Name))
	iconKey := strings.TrimSpace(req.IconKey)
	if name == "" || utf8.RuneCountInString(name) > maxTagNameRunes {
		return nil, errno.ErrInvalidParam.WithMessage("标签名称长度需在 1 至 24 个字符之间")
	}
	if iconKey != "" {
		if err := verifyIconObject(ctx, s.store, adminUserID, iconKey); err != nil {
			return nil, err
		}
	}

	tag := &model.Tag{
		Name:    name,
		IconKey: iconKey,
	}
	if err := s.metaRepo.CreateTag(ctx, tag); err != nil {
		if errors.Is(err, repository.ErrTagNameDuplicate) {
			return nil, errno.ErrInvalidParam.WithMessage("该标签已存在")
		}
		return nil, errno.ErrInternal.Wrap(err)
	}
	item := tag.ToItem(s.store.BuildRawURL(tag.IconKey))
	return &item, nil
}

// UpdateTag 编辑标签名称或图标。
func (s *AdminMetaService) UpdateTag(ctx context.Context, adminUserID, id uint64, req model.AdminUpdateTagRequest) (*model.TagItem, error) {
	if _, err := s.metaRepo.FindTagByID(ctx, id); err != nil {
		if errors.Is(err, repository.ErrTagNotFound) {
			return nil, errno.ErrNotFound.WithMessage("标签不存在")
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	fields := make(map[string]any)
	if req.Name != nil {
		name := strings.ToLower(strings.TrimSpace(*req.Name))
		if name == "" || utf8.RuneCountInString(name) > maxTagNameRunes {
			return nil, errno.ErrInvalidParam.WithMessage("标签名称长度需在 1 至 24 个字符之间")
		}
		fields["name"] = name
	}
	if req.IconKey != nil {
		iconKey := strings.TrimSpace(*req.IconKey)
		if iconKey != "" {
			if err := verifyIconObject(ctx, s.store, adminUserID, iconKey); err != nil {
				return nil, err
			}
		}
		fields["icon_key"] = iconKey
	}

	updated, err := s.metaRepo.UpdateTag(ctx, id, fields)
	if err != nil {
		if errors.Is(err, repository.ErrTagNameDuplicate) {
			return nil, errno.ErrInvalidParam.WithMessage("该标签已存在")
		}
		if errors.Is(err, repository.ErrTagNotFound) {
			return nil, errno.ErrNotFound.WithMessage("标签不存在")
		}
		return nil, errno.ErrInternal.Wrap(err)
	}
	item := updated.ToItem(s.store.BuildRawURL(updated.IconKey))
	return &item, nil
}

// DeleteTag 删除标签及其关联记录，并清理受影响作品的详情缓存。
func (s *AdminMetaService) DeleteTag(ctx context.Context, id uint64) error {
	affectedIDs, err := s.metaRepo.DeleteTag(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrTagNotFound) {
			return errno.ErrNotFound.WithMessage("标签不存在")
		}
		return errno.ErrInternal.Wrap(err)
	}
	s.invalidateWallpaperCaches(ctx, affectedIDs)
	return nil
}

func (s *AdminMetaService) invalidateWallpaperCaches(ctx context.Context, ids []uint64) {
	if s.wallpaperCache == nil || len(ids) == 0 {
		return
	}
	for _, wid := range ids {
		_ = s.wallpaperCache.Delete(ctx, wallpaperDetailKey(wid))
	}
}
