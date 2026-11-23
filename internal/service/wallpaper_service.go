package service

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/storage"
)

const (
	maxWallpaperTitleRunes = 80
	maxWallpaperDescRunes  = 500
	maxWallpaperTags       = 5
	maxTagNameRunes        = 24
)

var (
	categoryIDPattern = regexp.MustCompile(`^[1-9][0-9]*$`)
	likePrefixEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
)

// WallpaperService 负责分类/标签直查、壁纸投稿发布与作者软删。
type WallpaperService struct {
	metaRepo      *repository.MetaRepository
	wallpaperRepo *repository.WallpaperRepository
	userSvc       *UserService
	store         storage.StorageService
}

// NewWallpaperService 构造壁纸领域服务。
func NewWallpaperService(
	metaRepo *repository.MetaRepository,
	wallpaperRepo *repository.WallpaperRepository,
	userSvc *UserService,
	store storage.StorageService,
) *WallpaperService {
	return &WallpaperService{
		metaRepo:      metaRepo,
		wallpaperRepo: wallpaperRepo,
		userSvc:       userSvc,
		store:         store,
	}
}

// ListCategories 直查全站预置壁纸分类列表。
func (s *WallpaperService) ListCategories(ctx context.Context) (*model.CategoryList, error) {
	cats, err := s.metaRepo.ListCategories(ctx)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}
	items := make([]model.CategoryItem, 0, len(cats))
	for i := range cats {
		items = append(items, cats[i].ToItem())
	}
	return &model.CategoryList{Items: items}, nil
}

// ListTags 直查热门标签或按前缀联想检索标签。
func (s *WallpaperService) ListTags(ctx context.Context, rawQuery string, limit int) (*model.TagList, error) {
	q := strings.ToLower(strings.TrimSpace(rawQuery))
	if utf8.RuneCountInString(q) > maxTagNameRunes {
		return nil, errno.ErrInvalidParam.WithMessage("搜索词长度不能超过 24 个字符")
	}

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
		items = append(items, tags[i].ToItem())
	}
	return &model.TagList{Items: items}, nil
}

// CreateWallpaper 执行事务外参数/归属/?imageInfo 核验与方向/4K 计算，并在短事务内入库发布。
func (s *WallpaperService) CreateWallpaper(ctx context.Context, userID uint64, req model.CreateWallpaperRequest) (*model.WallpaperDetail, error) {
	title, err := validateWallpaperTitle(req.Title)
	if err != nil {
		return nil, err
	}
	desc, err := validateWallpaperDescription(req.Description)
	if err != nil {
		return nil, err
	}
	categoryID, err := parseCategoryID(req.CategoryID)
	if err != nil {
		return nil, err
	}
	tagNames, err := normalizeAndValidateTags(req.Tags)
	if err != nil {
		return nil, err
	}
	if !storage.ValidateObjectKey(storage.SceneWallpaper, userID, req.ObjectKey) {
		return nil, errno.ErrInvalidParam.WithMessage("object_key 不合法或无权使用")
	}

	category, err := s.metaRepo.FindCategoryByID(ctx, categoryID)
	if err != nil {
		if errors.Is(err, repository.ErrCategoryNotFound) {
			return nil, errno.ErrInvalidCategoryOrTag.WithMessage("指定分类不存在")
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	meta, err := verifyWallpaperObject(ctx, s.store, userID, req.ObjectKey)
	if err != nil {
		return nil, err
	}

	orientation := model.CalcOrientation(meta.Width, meta.Height)
	is4K := model.Is4KResolution(meta.Width, meta.Height)
	var is4KFlag uint8
	if is4K {
		is4KFlag = 1
	}

	wp := &model.Wallpaper{
		UserID:      userID,
		CategoryID:  category.ID,
		Title:       title,
		Description: desc,
		ObjectKey:   req.ObjectKey,
		Width:       meta.Width,
		Height:      meta.Height,
		Orientation: orientation,
		Is4K:        is4KFlag,
		SizeBytes:   uint64(meta.SizeBytes),
		Status:      model.WallpaperStatusActive,
	}
	attachedTags, err := s.wallpaperRepo.CreateWithTags(ctx, wp, tagNames)
	if err != nil {
		if errors.Is(err, repository.ErrDuplicateObjectKey) {
			return nil, errno.ErrDuplicateObjectKey
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	author, err := s.userSvc.GetAuthor(ctx, userID)
	if err != nil {
		return nil, err
	}

	tagItems := make([]model.TagItem, 0, len(attachedTags))
	for i := range attachedTags {
		tagItems = append(tagItems, attachedTags[i].ToItem())
	}

	return &model.WallpaperDetail{
		ID:            wp.ID,
		Title:         wp.Title,
		Description:   wp.Description,
		ThumbURL:      s.store.BuildThumbURL(wp.ObjectKey),
		RawURL:        s.store.BuildRawURL(wp.ObjectKey),
		Width:         wp.Width,
		Height:        wp.Height,
		Orientation:   wp.Orientation,
		Is4K:          is4K,
		SizeBytes:     wp.SizeBytes,
		Mime:          meta.MimeType,
		LikeCount:     0, // M6 由 wallpaper_stats 承载，新发布恒为 0
		FavCount:      0,
		DownloadCount: 0,
		CommentCount:  0,
		Category:      category.ToItem(),
		Tags:          tagItems,
		Author:        author,
		CreatedAt:     wp.CreatedAt.UTC(),
	}, nil
}

// DeleteWallpaper 校验作者归属并在短事务内软删壁纸及回收标签计数。
func (s *WallpaperService) DeleteWallpaper(ctx context.Context, userID, wallpaperID uint64) error {
	rec, err := s.wallpaperRepo.FindOwnerByID(ctx, wallpaperID)
	if err != nil {
		if errors.Is(err, repository.ErrWallpaperNotFound) {
			return errno.ErrWallpaperNotFound
		}
		return errno.ErrInternal.Wrap(err)
	}
	if rec.Status != model.WallpaperStatusActive {
		return errno.ErrWallpaperNotFound
	}
	if rec.UserID != userID {
		return errno.ErrForbidden
	}

	if err := s.wallpaperRepo.SoftDelete(ctx, wallpaperID); err != nil {
		if errors.Is(err, repository.ErrWallpaperNotFound) {
			return errno.ErrWallpaperNotFound
		}
		return errno.ErrInternal.Wrap(err)
	}
	return nil
}

func validateWallpaperTitle(raw string) (string, error) {
	title := strings.TrimSpace(raw)
	n := utf8.RuneCountInString(title)
	if n < 1 || n > maxWallpaperTitleRunes {
		return "", errno.ErrInvalidParam.WithMessage("壁纸标题长度需为 1~80 个字符")
	}
	return title, nil
}

func validateWallpaperDescription(raw string) (string, error) {
	desc := strings.TrimSpace(raw)
	if utf8.RuneCountInString(desc) > maxWallpaperDescRunes {
		return "", errno.ErrInvalidParam.WithMessage("壁纸简介长度不能超过 500 个字符")
	}
	return desc, nil
}

func parseCategoryID(raw string) (uint64, error) {
	if !categoryIDPattern.MatchString(raw) {
		return 0, errno.ErrInvalidParam.WithMessage("category_id 格式不合法")
	}
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		return 0, errno.ErrInvalidParam.WithMessage("category_id 格式不合法")
	}
	return id, nil
}

func normalizeAndValidateTags(rawTags []string) ([]string, error) {
	if len(rawTags) > maxWallpaperTags {
		return nil, errno.ErrInvalidCategoryOrTag.WithMessage("标签数量最多 5 个")
	}
	if len(rawTags) == 0 {
		return []string{}, nil
	}

	seen := make(map[string]struct{}, len(rawTags))
	result := make([]string, 0, len(rawTags))
	for _, raw := range rawTags {
		name := strings.ToLower(strings.TrimSpace(raw))
		n := utf8.RuneCountInString(name)
		if n < 1 || n > maxTagNameRunes {
			return nil, errno.ErrInvalidCategoryOrTag.WithMessage("单个标签长度需为 1~24 个字符")
		}
		for _, r := range name {
			if unicode.IsSpace(r) || r == ',' || r == '，' || unicode.IsControl(r) {
				return nil, errno.ErrInvalidCategoryOrTag.WithMessage("标签不能包含空白符、逗号或控制字符")
			}
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	return result, nil
}

func escapeLikePrefix(prefix string) string {
	return likePrefixEscaper.Replace(prefix) + "%"
}
