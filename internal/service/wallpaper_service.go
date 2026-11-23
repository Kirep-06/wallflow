package service

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/storage"
)

const (
	wallpaperDetailKeyPrefix = "wallpaper:detail:"
	wallpaperDetailHotL2TTL  = time.Hour
	wallpaperDetailColdL2TTL = 10 * time.Minute
	wallpaperHotAgeWindow    = 7 * 24 * time.Hour

	maxWallpaperTitleRunes = 80
	maxWallpaperDescRunes  = 500
	maxWallpaperTags       = 5
	maxTagNameRunes        = 24
)

var (
	categoryIDPattern = regexp.MustCompile(`^[1-9][0-9]*$`)
	likePrefixEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
)

// WallpaperDetailL2TTL 按壁纸发布时间计算冷热动态 L2 TTL（7 天内热图 1h，7 天外冷图 10m）。
func WallpaperDetailL2TTL(wp model.WallpaperDetailCache) time.Duration {
	if !wp.CreatedAt.IsZero() && time.Since(wp.CreatedAt) <= wallpaperHotAgeWindow {
		return wallpaperDetailHotL2TTL
	}
	return wallpaperDetailColdL2TTL
}

// WallpaperService 负责分类/标签直查、壁纸投稿软删、三级缓存详情以及冷热分离画廊列表。
type WallpaperService struct {
	metaRepo       *repository.MetaRepository
	wallpaperRepo  *repository.WallpaperRepository
	userSvc        *UserService
	wallpaperCache *cache.MultiLevel[model.WallpaperDetailCache]
	latestFeed     *cache.LatestFeed
	store          storage.StorageService
}

// NewWallpaperService 构造壁纸领域服务。
func NewWallpaperService(
	metaRepo *repository.MetaRepository,
	wallpaperRepo *repository.WallpaperRepository,
	userSvc *UserService,
	wallpaperCache *cache.MultiLevel[model.WallpaperDetailCache],
	latestFeed *cache.LatestFeed,
	store storage.StorageService,
) *WallpaperService {
	return &WallpaperService{
		metaRepo:       metaRepo,
		wallpaperRepo:  wallpaperRepo,
		userSvc:        userSvc,
		wallpaperCache: wallpaperCache,
		latestFeed:     latestFeed,
		store:          store,
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
	attachedTags, err := s.wallpaperRepo.CreateWithTags(ctx, wp, tagNames, nil)
	if err != nil {
		if errors.Is(err, repository.ErrDuplicateObjectKey) {
			return nil, errno.ErrDuplicateObjectKey
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	// 写库后联动缓存：清潜在空值穿透哨兵，并条件追加进最新流热窗口（失败仅告警不阻断投稿）。
	s.invalidateDetailCache(ctx, wp.ID)
	s.pushLatestFeed(ctx, wp.ID)

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

	// 软删后先删 L1 + sf.Forget 再删 L2，并从最新流热窗口移除（失败仅告警不回滚事务）。
	s.invalidateDetailCache(ctx, wallpaperID)
	s.removeLatestFeed(ctx, wallpaperID)
	return nil
}

// GetWallpaperDetail 通过三级缓存获取壁纸公开详情并动态组装原图/缩略图 URL 与作者信息。
func (s *WallpaperService) GetWallpaperDetail(ctx context.Context, wallpaperID uint64) (*model.WallpaperDetail, error) {
	cached, err := s.getCachedDetail(ctx, wallpaperID)
	if err != nil {
		if errors.Is(err, cache.ErrNotFound) {
			return nil, errno.ErrWallpaperNotFound
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	author, err := s.userSvc.GetAuthor(ctx, cached.UserID)
	if err != nil {
		if errors.Is(err, errno.ErrUserNotFound) {
			author = s.fallbackAuthor(cached.UserID)
		} else {
			return nil, err
		}
	}

	mime, _ := storage.MimeFromObjectKey(cached.ObjectKey)
	tags := cached.Tags
	if tags == nil {
		tags = []model.TagItem{}
	}

	return &model.WallpaperDetail{
		ID:            cached.ID,
		Title:         cached.Title,
		Description:   cached.Description,
		ThumbURL:      s.store.BuildThumbURL(cached.ObjectKey),
		RawURL:        s.store.BuildRawURL(cached.ObjectKey),
		Width:         cached.Width,
		Height:        cached.Height,
		Orientation:   cached.Orientation,
		Is4K:          cached.Is4K,
		SizeBytes:     cached.SizeBytes,
		Mime:          mime,
		LikeCount:     0, // M6 由 wallpaper_stats 承载
		FavCount:      0,
		DownloadCount: 0,
		CommentCount:  0,
		Category:      cached.Category,
		Tags:          tags,
		Author:        author,
		CreatedAt:     cached.CreatedAt.UTC(),
	}, nil
}

// ListWallpapers 返回发现页最新流（热窗口冷热分离）或按分类/标签单维互斥筛选的画廊瀑布流。
func (s *WallpaperService) ListWallpapers(ctx context.Context, query model.WallpaperListQuery) (*response.PageData[model.WallpaperSummary], error) {
	if query.CategoryID > 0 && query.TagID > 0 {
		return nil, errno.ErrInvalidParam.WithMessage("category_id 与 tag_id 不能同时传入")
	}
	limit, err := normalizeListLimit(query.Limit)
	if err != nil {
		return nil, err
	}
	beforeID, err := parseIDCursor(query.Cursor)
	if err != nil {
		return nil, err
	}

	want := limit + 1
	var rawIDs []uint64
	switch {
	case query.CategoryID > 0:
		rawIDs, err = s.wallpaperRepo.ListCategoryWallpaperIDs(ctx, query.CategoryID, beforeID, want)
	case query.TagID > 0:
		rawIDs, err = s.wallpaperRepo.ListTagWallpaperIDs(ctx, query.TagID, beforeID, want)
	default:
		rawIDs, err = s.fetchLatestPageIDs(ctx, beforeID, want)
	}
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	pageIDs, nextCursor, hasMore, err := paginateIDs(rawIDs, limit)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}
	return s.assembleSummaryPage(ctx, pageIDs, nextCursor, hasMore, nil)
}

// ListUserWallpapers 返回指定创作者公开主页的已发布壁纸流（用户不存在返回 404/20003）。
func (s *WallpaperService) ListUserWallpapers(ctx context.Context, targetUserID uint64, rawCursor string, rawLimit int) (*response.PageData[model.WallpaperSummary], error) {
	limit, err := normalizeListLimit(rawLimit)
	if err != nil {
		return nil, err
	}
	beforeID, err := parseIDCursor(rawCursor)
	if err != nil {
		return nil, err
	}

	// 复用用户资料三级缓存校验创作者存在性（自带空值防穿透）并免去后续重复查作者。
	author, err := s.userSvc.GetAuthor(ctx, targetUserID)
	if err != nil {
		return nil, err
	}

	rawIDs, err := s.wallpaperRepo.ListUserWallpaperIDs(ctx, targetUserID, beforeID, limit+1)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	pageIDs, nextCursor, hasMore, err := paginateIDs(rawIDs, limit)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}
	return s.assembleSummaryPage(ctx, pageIDs, nextCursor, hasMore, &author)
}

func (s *WallpaperService) getCachedDetail(ctx context.Context, wallpaperID uint64) (model.WallpaperDetailCache, error) {
	return s.wallpaperCache.Get(ctx, wallpaperDetailKey(wallpaperID), func(loadCtx context.Context) (model.WallpaperDetailCache, error) {
		found, err := s.wallpaperRepo.FindDetailsByIDs(loadCtx, []uint64{wallpaperID})
		if err != nil {
			return model.WallpaperDetailCache{}, err
		}
		detail, ok := found[wallpaperID]
		if !ok {
			return model.WallpaperDetailCache{}, cache.ErrNotFound
		}
		return detail, nil
	})
}

func (s *WallpaperService) fetchLatestPageIDs(ctx context.Context, beforeID uint64, want int) ([]uint64, error) {
	return fetchLatestIDsWithFallback(
		beforeID,
		want,
		func(cursorID uint64, count int) ([]uint64, bool, error) {
			if s.latestFeed == nil {
				return nil, false, nil
			}
			ids, ready, err := s.latestFeed.ReadPage(ctx, cursorID, count)
			if err != nil {
				slog.WarnContext(ctx, "latest feed read degraded to mysql",
					slog.Uint64("before_id", cursorID),
					slog.Any("err", err),
				)
				return nil, false, nil
			}
			return ids, ready, nil
		},
		func(cursorID uint64, count int) ([]uint64, error) {
			return s.wallpaperRepo.ListLatestIDs(ctx, cursorID, count)
		},
	)
}

// fetchLatestIDsWithFallback 编排热窗口读取与热冷交界无缝 MySQL 回源补齐。
func fetchLatestIDsWithFallback(
	beforeID uint64,
	want int,
	readHot func(beforeID uint64, count int) ([]uint64, bool, error),
	readCold func(beforeID uint64, count int) ([]uint64, error),
) ([]uint64, error) {
	if want <= 0 {
		return []uint64{}, nil
	}
	hotIDs, ready, err := readHot(beforeID, want)
	if err != nil || !ready {
		return readCold(beforeID, want)
	}
	if len(hotIDs) >= want {
		return hotIDs[:want], nil
	}

	coldBefore := beforeID
	if len(hotIDs) > 0 {
		coldBefore = hotIDs[len(hotIDs)-1]
	}
	coldIDs, err := readCold(coldBefore, want-len(hotIDs))
	if err != nil {
		return nil, err
	}
	if len(hotIDs) == 0 {
		return coldIDs, nil
	}
	merged := make([]uint64, 0, len(hotIDs)+len(coldIDs))
	merged = append(merged, hotIDs...)
	merged = append(merged, coldIDs...)
	return merged, nil
}

func (s *WallpaperService) assembleSummaryPage(
	ctx context.Context,
	pageIDs []uint64,
	nextCursor string,
	hasMore bool,
	fixedAuthor *model.WallpaperAuthor,
) (*response.PageData[model.WallpaperSummary], error) {
	if len(pageIDs) == 0 {
		page := response.NewPageData[model.WallpaperSummary](nil, nextCursor, hasMore)
		return &page, nil
	}

	keys := make([]string, 0, len(pageIDs))
	keyToID := make(map[string]uint64, len(pageIDs))
	for _, id := range pageIDs {
		k := wallpaperDetailKey(id)
		if _, dup := keyToID[k]; dup {
			continue
		}
		keyToID[k] = id
		keys = append(keys, k)
	}

	detailsMap, err := s.wallpaperCache.MGet(ctx, keys, func(loadCtx context.Context, missKeys []string) (map[string]model.WallpaperDetailCache, error) {
		missIDs := make([]uint64, 0, len(missKeys))
		for _, k := range missKeys {
			if id, ok := keyToID[k]; ok {
				missIDs = append(missIDs, id)
			}
		}
		found, err := s.wallpaperRepo.FindDetailsByIDs(loadCtx, missIDs)
		if err != nil {
			return nil, err
		}
		out := make(map[string]model.WallpaperDetailCache, len(found))
		for id, d := range found {
			out[wallpaperDetailKey(id)] = d
		}
		return out, nil
	})
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	// 按原序列保序过滤已软删/不存在的幽灵 ID，并收拢去重作者 ID。
	ordered := make([]model.WallpaperDetailCache, 0, len(pageIDs))
	var userIDs []uint64
	if fixedAuthor == nil {
		userIDs = make([]uint64, 0, len(pageIDs))
	}
	seenUsers := make(map[uint64]struct{}, len(pageIDs))
	for _, id := range pageIDs {
		d, ok := detailsMap[wallpaperDetailKey(id)]
		if !ok {
			continue
		}
		ordered = append(ordered, d)
		if fixedAuthor == nil {
			if _, dup := seenUsers[d.UserID]; !dup {
				seenUsers[d.UserID] = struct{}{}
				userIDs = append(userIDs, d.UserID)
			}
		}
	}

	var authors map[uint64]model.WallpaperAuthor
	if fixedAuthor == nil && len(userIDs) > 0 {
		authors, err = s.userSvc.GetAuthorsByIDs(ctx, userIDs)
		if err != nil {
			return nil, err
		}
	}

	items := make([]model.WallpaperSummary, 0, len(ordered))
	for i := range ordered {
		wp := ordered[i]
		var author model.WallpaperAuthor
		if fixedAuthor != nil {
			author = *fixedAuthor
		} else if a, ok := authors[wp.UserID]; ok {
			author = a
		} else {
			author = s.fallbackAuthor(wp.UserID)
		}

		items = append(items, model.WallpaperSummary{
			ID:            wp.ID,
			Title:         wp.Title,
			ThumbURL:      s.store.BuildThumbURL(wp.ObjectKey),
			Width:         wp.Width,
			Height:        wp.Height,
			Orientation:   wp.Orientation,
			Is4K:          wp.Is4K,
			LikeCount:     0, // M6 由 wallpaper_stats 承载
			FavCount:      0,
			DownloadCount: 0,
			CommentCount:  0,
			Category:      wp.Category,
			Author:        author,
			CreatedAt:     wp.CreatedAt.UTC(),
		})
	}

	page := response.NewPageData(items, nextCursor, hasMore)
	return &page, nil
}

func (s *WallpaperService) invalidateDetailCache(ctx context.Context, wallpaperID uint64) {
	if s.wallpaperCache == nil || wallpaperID == 0 {
		return
	}
	if err := s.wallpaperCache.Delete(ctx, wallpaperDetailKey(wallpaperID)); err != nil {
		slog.WarnContext(ctx, "invalidate wallpaper detail cache failed",
			slog.Uint64("wallpaper_id", wallpaperID),
			slog.Any("err", err),
		)
	}
}

func (s *WallpaperService) pushLatestFeed(ctx context.Context, wallpaperID uint64) {
	if s.latestFeed == nil || wallpaperID == 0 {
		return
	}
	if err := s.latestFeed.PushIfReady(ctx, wallpaperID); err != nil {
		slog.WarnContext(ctx, "push latest feed failed",
			slog.Uint64("wallpaper_id", wallpaperID),
			slog.Any("err", err),
		)
	}
}

func (s *WallpaperService) removeLatestFeed(ctx context.Context, wallpaperID uint64) {
	if s.latestFeed == nil || wallpaperID == 0 {
		return
	}
	if err := s.latestFeed.Remove(ctx, wallpaperID); err != nil {
		slog.WarnContext(ctx, "remove latest feed failed",
			slog.Uint64("wallpaper_id", wallpaperID),
			slog.Any("err", err),
		)
	}
}

func (s *WallpaperService) fallbackAuthor(userID uint64) model.WallpaperAuthor {
	var avatarURL string
	if s.userSvc != nil {
		avatarURL = s.userSvc.BuildAvatarURL("")
	}
	return model.WallpaperAuthor{
		ID:        userID,
		AvatarURL: avatarURL,
	}
}

func paginateIDs(rawIDs []uint64, limit int) (pageIDs []uint64, nextCursor string, hasMore bool, err error) {
	if len(rawIDs) <= limit {
		return rawIDs, "", false, nil
	}
	pageIDs = rawIDs[:limit]
	nextCursor, err = cursor.Encode(cursor.IDCursor{ID: pageIDs[len(pageIDs)-1]})
	if err != nil {
		return nil, "", false, err
	}
	return pageIDs, nextCursor, true, nil
}

func wallpaperDetailKey(wallpaperID uint64) string {
	return wallpaperDetailKeyPrefix + strconv.FormatUint(wallpaperID, 10)
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
