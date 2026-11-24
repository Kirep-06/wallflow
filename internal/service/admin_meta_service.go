package service

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/storage"
)

var categorySlugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)

const (
	maxCategoryNameRunes = 32
	maxCategoryDescRunes = 300
	defaultAdminTagLimit = 200
)

// AdminMetaService 负责管理后台的鉴权校验与分类/话题（标签）增删改业务。
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

// ListCategories 返回全部分类及其名下正常壁纸数量。
func (s *AdminMetaService) ListCategories(ctx context.Context) (*model.AdminCategoryList, error) {
	cats, err := s.metaRepo.ListCategories(ctx)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}
	counts, err := s.metaRepo.CountWallpapersByCategory(ctx)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}
	items := make([]model.AdminCategoryItem, 0, len(cats))
	for i := range cats {
		items = append(items, model.AdminCategoryItem{
			CategoryItem:   cats[i].ToItem(s.store.BuildRawURL(cats[i].IconKey)),
			WallpaperCount: counts[cats[i].ID],
		})
	}
	return &model.AdminCategoryList{Items: items}, nil
}

// CreateCategory 新增分类（支持一级大分类 parent_id=0 或归属大类下的二级小分类、简介与七牛云图标）。
func (s *AdminMetaService) CreateCategory(ctx context.Context, adminUserID uint64, req model.AdminCreateCategoryRequest) (*model.AdminCategoryItem, error) {
	slug := strings.ToLower(strings.TrimSpace(req.Slug))
	name := strings.TrimSpace(req.Name)
	desc := strings.TrimSpace(req.Description)
	iconKey := strings.TrimSpace(req.IconKey)

	if !categorySlugPattern.MatchString(slug) {
		return nil, errno.ErrInvalidParam.WithMessage("分类标识 slug 仅支持小写字母、数字和短横线（1-32 字符）")
	}
	if name == "" || utf8.RuneCountInString(name) > maxCategoryNameRunes {
		return nil, errno.ErrInvalidParam.WithMessage("分类名称长度需在 1 至 32 个字符之间")
	}
	if utf8.RuneCountInString(desc) > maxCategoryDescRunes {
		return nil, errno.ErrInvalidParam.WithMessage("分类简介不能超过 300 个字符")
	}

	parentID, err := s.parseAndValidateParentID(ctx, req.ParentID, 0)
	if err != nil {
		return nil, err
	}

	if iconKey != "" {
		if err := verifyIconObject(ctx, s.store, adminUserID, iconKey); err != nil {
			return nil, err
		}
	}

	cat := &model.Category{
		ParentID:    parentID,
		Slug:        slug,
		Name:        name,
		Description: desc,
		SortNo:      req.SortNo,
		IconKey:     iconKey,
	}
	if err := s.metaRepo.CreateCategory(ctx, cat); err != nil {
		if errors.Is(err, repository.ErrCategorySlugDuplicate) {
			return nil, errno.ErrInvalidParam.WithMessage("分类标识 slug 已存在")
		}
		return nil, errno.ErrInternal.Wrap(err)
	}
	return &model.AdminCategoryItem{
		CategoryItem:   cat.ToItem(s.store.BuildRawURL(cat.IconKey)),
		WallpaperCount: 0,
	}, nil
}

// UpdateCategory 编辑分类名称、标识、父级大分类、简介、排序值或图标。
func (s *AdminMetaService) UpdateCategory(ctx context.Context, adminUserID, id uint64, req model.AdminUpdateCategoryRequest) (*model.AdminCategoryItem, error) {
	if _, err := s.metaRepo.FindCategoryByID(ctx, id); err != nil {
		if errors.Is(err, repository.ErrCategoryNotFound) {
			return nil, errno.ErrNotFound.WithMessage("分类不存在")
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	fields := make(map[string]any)
	if req.ParentID != nil {
		parentID, err := s.parseAndValidateParentID(ctx, *req.ParentID, id)
		if err != nil {
			return nil, err
		}
		fields["parent_id"] = parentID
	}
	if req.Slug != nil {
		slug := strings.ToLower(strings.TrimSpace(*req.Slug))
		if !categorySlugPattern.MatchString(slug) {
			return nil, errno.ErrInvalidParam.WithMessage("分类标识 slug 仅支持小写字母、数字和短横线（1-32 字符）")
		}
		fields["slug"] = slug
	}
	if req.Name != nil {
		name := strings.TrimSpace(*req.Name)
		if name == "" || utf8.RuneCountInString(name) > maxCategoryNameRunes {
			return nil, errno.ErrInvalidParam.WithMessage("分类名称长度需在 1 至 32 个字符之间")
		}
		fields["name"] = name
	}
	if req.Description != nil {
		desc := strings.TrimSpace(*req.Description)
		if utf8.RuneCountInString(desc) > maxCategoryDescRunes {
			return nil, errno.ErrInvalidParam.WithMessage("分类简介不能超过 300 个字符")
		}
		fields["description"] = desc
	}
	if req.SortNo != nil {
		fields["sort_no"] = *req.SortNo
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

	updated, err := s.metaRepo.UpdateCategory(ctx, id, fields)
	if err != nil {
		if errors.Is(err, repository.ErrCategorySlugDuplicate) {
			return nil, errno.ErrInvalidParam.WithMessage("分类标识 slug 已存在")
		}
		if errors.Is(err, repository.ErrCategoryNotFound) {
			return nil, errno.ErrNotFound.WithMessage("分类不存在")
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	counts, _ := s.metaRepo.CountWallpapersByCategory(ctx)
	return &model.AdminCategoryItem{
		CategoryItem:   updated.ToItem(s.store.BuildRawURL(updated.IconKey)),
		WallpaperCount: counts[updated.ID],
	}, nil
}

// DeleteCategory 安全删除分类：大类下有子小类禁止直删，名下有壁纸时必须指定迁移目标分类。
func (s *AdminMetaService) DeleteCategory(ctx context.Context, id, migrateToID uint64) error {
	affectedIDs, err := s.metaRepo.DeleteCategoryWithMigrate(ctx, id, migrateToID)
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrCategoryNotFound):
			return errno.ErrNotFound.WithMessage("分类或目标迁移分类不存在")
		case errors.Is(err, repository.ErrCategoryHasChildren):
			return errno.ErrInvalidParam.WithMessage("该大分类下仍有小分类板块，请先删除或移走小分类")
		case errors.Is(err, repository.ErrCategoryHasWallpapers):
			return errno.ErrInvalidParam.WithMessage("该分类下仍有壁纸，请指定迁移目标分类后再删除")
		default:
			return errno.ErrInternal.Wrap(err)
		}
	}
	s.invalidateWallpaperCaches(ctx, affectedIDs)
	return nil
}

func (s *AdminMetaService) parseAndValidateParentID(ctx context.Context, raw string, selfID uint64) (uint64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "0" {
		return 0, nil
	}
	pid, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return 0, errno.ErrInvalidParam.WithMessage("父级大分类 ID 非法")
	}
	if pid == 0 {
		return 0, nil
	}
	if selfID > 0 && pid == selfID {
		return 0, errno.ErrInvalidParam.WithMessage("父级分类不能指向自身")
	}
	parent, err := s.metaRepo.FindCategoryByID(ctx, pid)
	if err != nil {
		if errors.Is(err, repository.ErrCategoryNotFound) {
			return 0, errno.ErrInvalidParam.WithMessage("所选父级大分类不存在")
		}
		return 0, errno.ErrInternal.Wrap(err)
	}
	if parent.ParentID != 0 {
		return 0, errno.ErrInvalidParam.WithMessage("仅支持两级分类，请选择一级大分类作为父级")
	}
	return pid, nil
}

// ListTags 返回后台话题/标签列表（支持关键词过滤）。
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

// CreateTag 新增话题/标签。
func (s *AdminMetaService) CreateTag(ctx context.Context, adminUserID uint64, req model.AdminCreateTagRequest) (*model.TagItem, error) {
	name := strings.ToLower(strings.TrimSpace(req.Name))
	iconKey := strings.TrimSpace(req.IconKey)
	if name == "" || utf8.RuneCountInString(name) > maxTagNameRunes {
		return nil, errno.ErrInvalidParam.WithMessage("话题名称长度需在 1 至 24 个字符之间")
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
			return nil, errno.ErrInvalidParam.WithMessage("该话题已存在")
		}
		return nil, errno.ErrInternal.Wrap(err)
	}
	item := tag.ToItem(s.store.BuildRawURL(tag.IconKey))
	return &item, nil
}

// UpdateTag 编辑话题/标签名称或图标。
func (s *AdminMetaService) UpdateTag(ctx context.Context, adminUserID, id uint64, req model.AdminUpdateTagRequest) (*model.TagItem, error) {
	if _, err := s.metaRepo.FindTagByID(ctx, id); err != nil {
		if errors.Is(err, repository.ErrTagNotFound) {
			return nil, errno.ErrNotFound.WithMessage("话题不存在")
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	fields := make(map[string]any)
	if req.Name != nil {
		name := strings.ToLower(strings.TrimSpace(*req.Name))
		if name == "" || utf8.RuneCountInString(name) > maxTagNameRunes {
			return nil, errno.ErrInvalidParam.WithMessage("话题名称长度需在 1 至 24 个字符之间")
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
			return nil, errno.ErrInvalidParam.WithMessage("该话题已存在")
		}
		if errors.Is(err, repository.ErrTagNotFound) {
			return nil, errno.ErrNotFound.WithMessage("话题不存在")
		}
		return nil, errno.ErrInternal.Wrap(err)
	}
	item := updated.ToItem(s.store.BuildRawURL(updated.IconKey))
	return &item, nil
}

// DeleteTag 删除话题及其关联记录，并清理受影响壁纸的详情缓存。
func (s *AdminMetaService) DeleteTag(ctx context.Context, id uint64) error {
	affectedIDs, err := s.metaRepo.DeleteTag(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrTagNotFound) {
			return errno.ErrNotFound.WithMessage("话题不存在")
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
		_ = s.wallpaperCache.Delete(ctx, strconv.FormatUint(wid, 10))
	}
}
