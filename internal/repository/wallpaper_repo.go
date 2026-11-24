package repository

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"wallpaper-feed/internal/model"
)

var (
	ErrWallpaperNotFound  = errors.New("repository: wallpaper not found")
	ErrDuplicateObjectKey = errors.New("repository: duplicate object key")
)

// WallpaperRepository 封装壁纸投稿入库、作者软删与计数查询的数据访问。
type WallpaperRepository struct {
	db *gorm.DB
}

// NewWallpaperRepository 构造壁纸仓储实例。
func NewWallpaperRepository(db *gorm.DB) *WallpaperRepository {
	return &WallpaperRepository{db: db}
}

// CreateWithTags 保持向后兼容的单图投稿入口。
func (r *WallpaperRepository) CreateWithTags(
	ctx context.Context,
	wp *model.Wallpaper,
	tagNames []string,
	buildOutbox func(*model.Wallpaper) (*model.OutboxEvent, error),
) ([]model.Tag, error) {
	defaultImg := model.WallpaperImage{
		SortNo:      0,
		ObjectKey:   wp.ObjectKey,
		Width:       wp.Width,
		Height:      wp.Height,
		Orientation: wp.Orientation,
		Is4K:        wp.Is4K,
		SizeBytes:   wp.SizeBytes,
	}
	return r.CreateWithTagsAndImages(ctx, wp, []model.WallpaperImage{defaultImg}, tagNames, buildOutbox)
}

// CreateWithTagsAndImages 在单次短事务内写入壁纸主表、多图子表（wallpaper_images）、按需建标签递增计数，
// 当 buildOutbox 非空时在同一本地事务内写入 outbox_events 实现 Transactional Outbox。
func (r *WallpaperRepository) CreateWithTagsAndImages(
	ctx context.Context,
	wp *model.Wallpaper,
	images []model.WallpaperImage,
	tagNames []string,
	buildOutbox func(*model.Wallpaper) (*model.OutboxEvent, error),
) ([]model.Tag, error) {
	attached := make([]model.Tag, 0, len(tagNames))
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(wp).Error; err != nil {
			if isDuplicateKeyError(err) {
				return ErrDuplicateObjectKey
			}
			return fmt.Errorf("wallpaper repo create: %w", err)
		}

		if len(images) > 0 {
			for i := range images {
				images[i].WallpaperID = wp.ID
				images[i].SortNo = uint32(i)
			}
			if err := tx.Create(&images).Error; err != nil {
				if isDuplicateKeyError(err) {
					return ErrDuplicateObjectKey
				}
				return fmt.Errorf("wallpaper repo create images: %w", err)
			}
		}

		if len(tagNames) > 0 {
			seedTags := make([]model.Tag, len(tagNames))
			for i, name := range tagNames {
				seedTags[i] = model.Tag{Name: name}
			}
			if err := tx.Clauses(clause.Insert{Modifier: "IGNORE"}).Select("Name").Create(&seedTags).Error; err != nil {
				return fmt.Errorf("wallpaper repo insert ignore tags: %w", err)
			}

			var tags []model.Tag
			if err := tx.Select("id, name, use_count").Where("name IN ?", tagNames).Find(&tags).Error; err != nil {
				return fmt.Errorf("wallpaper repo query tags: %w", err)
			}

			links := make([]model.WallpaperTag, 0, len(tags))
			tagIDs := make([]uint64, 0, len(tags))
			for _, t := range tags {
				links = append(links, model.WallpaperTag{WallpaperID: wp.ID, TagID: t.ID})
				tagIDs = append(tagIDs, t.ID)
			}
			if len(links) > 0 {
				if err := tx.Create(&links).Error; err != nil {
					return fmt.Errorf("wallpaper repo insert wallpaper_tags: %w", err)
				}
				slices.Sort(tagIDs)
				if err := tx.Model(&model.Tag{}).
					Where("id IN ?", tagIDs).
					UpdateColumn("use_count", gorm.Expr("use_count + 1")).Error; err != nil {
					return fmt.Errorf("wallpaper repo increment tag use_count: %w", err)
				}

				byName := make(map[string]model.Tag, len(tags))
				for _, t := range tags {
					t.UseCount++
					byName[t.Name] = t
				}
				for _, name := range tagNames {
					if t, ok := byName[name]; ok {
						attached = append(attached, t)
					}
				}
			}
		}

		if buildOutbox != nil {
			ev, err := buildOutbox(wp)
			if err != nil {
				return fmt.Errorf("wallpaper repo build outbox event: %w", err)
			}
			if ev != nil {
				if err := tx.Create(ev).Error; err != nil {
					return fmt.Errorf("wallpaper repo insert outbox event: %w", err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return attached, nil
}

// IsActive 核验指定壁纸当前是否处于正常可见状态（供异步扇出消费前防软删幽灵写回）。
func (r *WallpaperRepository) IsActive(ctx context.Context, id uint64) (bool, error) {
	if id == 0 {
		return false, nil
	}
	var status uint8
	err := r.db.WithContext(ctx).
		Table(model.Wallpaper{}.TableName()).
		Select("status").
		Where("id = ?", id).
		Take(&status).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("wallpaper repo is active: %w", err)
	}
	return status == model.WallpaperStatusActive, nil
}

// FindOwnerByID 查询壁纸归属与状态，用于作者软删前校验。
func (r *WallpaperRepository) FindOwnerByID(ctx context.Context, id uint64) (*model.WallpaperOwnerRecord, error) {
	var rec model.WallpaperOwnerRecord
	err := r.db.WithContext(ctx).
		Table(model.Wallpaper{}.TableName()).
		Select("id, user_id, status").
		Where("id = ?", id).
		Take(&rec).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrWallpaperNotFound
		}
		return nil, fmt.Errorf("wallpaper repo find owner by id: %w", err)
	}
	return &rec, nil
}

// SoftDelete 在短事务内软删正常状态的壁纸并安全扣减关联标签计数。
func (r *WallpaperRepository) SoftDelete(ctx context.Context, id uint64) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&model.Wallpaper{}).
			Where("id = ? AND status = ?", id, model.WallpaperStatusActive).
			Update("status", model.WallpaperStatusDeleted)
		if res.Error != nil {
			return fmt.Errorf("wallpaper repo soft delete: %w", res.Error)
		}
		if res.RowsAffected != 1 {
			return ErrWallpaperNotFound
		}

		var tagIDs []uint64
		if err := tx.Table(model.WallpaperTag{}.TableName()).
			Select("tag_id").
			Where("wallpaper_id = ?", id).
			Pluck("tag_id", &tagIDs).Error; err != nil {
			return fmt.Errorf("wallpaper repo list tag ids: %w", err)
		}
		if len(tagIDs) > 0 {
			slices.Sort(tagIDs)
			if err := tx.Model(&model.Tag{}).
				Where("id IN ?", tagIDs).
				UpdateColumn("use_count", gorm.Expr("CASE WHEN use_count > 0 THEN use_count - 1 ELSE 0 END")).Error; err != nil {
				return fmt.Errorf("wallpaper repo decrement tag use_count: %w", err)
			}
		}
		return nil
	})
}

// CountActiveByUserID 借助覆盖索引 idx_user_status_id 直查用户已发布的正常壁纸总数。
func (r *WallpaperRepository) CountActiveByUserID(ctx context.Context, userID uint64) (int64, error) {
	var total int64
	if err := r.db.WithContext(ctx).
		Model(&model.Wallpaper{}).
		Where("user_id = ? AND status = ?", userID, model.WallpaperStatusActive).
		Count(&total).Error; err != nil {
		return 0, fmt.Errorf("wallpaper repo count active by user: %w", err)
	}
	return total, nil
}

// FindDetailsByIDs 按主键批量回源正常壁纸的静态详情快照（壁纸主表 + 分类 + 关联标签）。
func (r *WallpaperRepository) FindDetailsByIDs(ctx context.Context, ids []uint64) (map[uint64]model.WallpaperDetailCache, error) {
	if len(ids) == 0 {
		return map[uint64]model.WallpaperDetailCache{}, nil
	}

	var wps []model.Wallpaper
	if err := r.db.WithContext(ctx).
		Select("id, user_id, category_id, title, description, object_key, width, height, orientation, is_4k, size_bytes, created_at").
		Where("id IN ? AND status = ?", ids, model.WallpaperStatusActive).
		Find(&wps).Error; err != nil {
		return nil, fmt.Errorf("wallpaper repo find details by ids: %w", err)
	}
	if len(wps) == 0 {
		return map[uint64]model.WallpaperDetailCache{}, nil
	}

	wpIDs := make([]uint64, 0, len(wps))
	catIDSet := make(map[uint64]struct{}, len(wps))
	catIDs := make([]uint64, 0, len(wps))
	for i := range wps {
		wpIDs = append(wpIDs, wps[i].ID)
		if _, exists := catIDSet[wps[i].CategoryID]; !exists {
			catIDSet[wps[i].CategoryID] = struct{}{}
			catIDs = append(catIDs, wps[i].CategoryID)
		}
	}

	var cats []model.Category
	if err := r.db.WithContext(ctx).
		Select("id, parent_id, slug, name, description, sort_no, icon_key").
		Where("id IN ?", catIDs).
		Find(&cats).Error; err != nil {
		return nil, fmt.Errorf("wallpaper repo find categories by ids: %w", err)
	}
	catByID := make(map[uint64]model.CategoryItem, len(cats))
	for i := range cats {
		catByID[cats[i].ID] = cats[i].ToItem()
	}

	type wallpaperTagRow struct {
		WallpaperID uint64
		ID          uint64
		Name        string
		UseCount    uint64
	}
	var tagRows []wallpaperTagRow
	if err := r.db.WithContext(ctx).
		Table(model.WallpaperTag{}.TableName()+" AS wt").
		Select("wt.wallpaper_id, t.id, t.name, t.use_count").
		Joins("JOIN "+model.Tag{}.TableName()+" AS t ON t.id = wt.tag_id").
		Where("wt.wallpaper_id IN ?", wpIDs).
		Order("wt.wallpaper_id ASC, t.id ASC").
		Scan(&tagRows).Error; err != nil {
		return nil, fmt.Errorf("wallpaper repo find tags by wallpaper ids: %w", err)
	}
	tagsByWP := make(map[uint64][]model.TagItem, len(wps))
	for _, row := range tagRows {
		tagsByWP[row.WallpaperID] = append(tagsByWP[row.WallpaperID], model.TagItem{
			ID:       row.ID,
			Name:     row.Name,
			UseCount: row.UseCount,
		})
	}

	var imgRows []model.WallpaperImage
	if err := r.db.WithContext(ctx).
		Where("wallpaper_id IN ?", wpIDs).
		Order("wallpaper_id ASC, sort_no ASC, id ASC").
		Find(&imgRows).Error; err != nil {
		return nil, fmt.Errorf("wallpaper repo find images by wallpaper ids: %w", err)
	}
	imgsByWP := make(map[uint64][]model.WallpaperImageCache, len(wps))
	for _, img := range imgRows {
		imgsByWP[img.WallpaperID] = append(imgsByWP[img.WallpaperID], model.WallpaperImageCache{
			ID:          img.ID,
			SortNo:      img.SortNo,
			ObjectKey:   img.ObjectKey,
			Width:       img.Width,
			Height:      img.Height,
			Orientation: img.Orientation,
			Is4K:        img.Is4K == 1,
			SizeBytes:   img.SizeBytes,
		})
	}

	result := make(map[uint64]model.WallpaperDetailCache, len(wps))
	for i := range wps {
		w := wps[i]
		tags := tagsByWP[w.ID]
		if tags == nil {
			tags = []model.TagItem{}
		}
		imgs := imgsByWP[w.ID]
		if len(imgs) == 0 {
			imgs = []model.WallpaperImageCache{
				{
					ID:          w.ID,
					SortNo:      0,
					ObjectKey:   w.ObjectKey,
					Width:       w.Width,
					Height:      w.Height,
					Orientation: w.Orientation,
					Is4K:        w.Is4K == 1,
					SizeBytes:   w.SizeBytes,
				},
			}
		}
		result[w.ID] = model.WallpaperDetailCache{
			ID:          w.ID,
			UserID:      w.UserID,
			CategoryID:  w.CategoryID,
			Title:       w.Title,
			Description: w.Description,
			ObjectKey:   w.ObjectKey,
			Width:       w.Width,
			Height:      w.Height,
			Orientation: w.Orientation,
			Is4K:        w.Is4K == 1,
			SizeBytes:   w.SizeBytes,
			Category:    catByID[w.CategoryID],
			Tags:        tags,
			Images:      imgs,
			CreatedAt:   w.CreatedAt.UTC(),
		}
	}
	return result, nil
}

// ListLatestIDs 命中覆盖索引 idx_status_id 倒序游标查询全站正常壁纸 ID。
func (r *WallpaperRepository) ListLatestIDs(ctx context.Context, beforeID uint64, limit int) ([]uint64, error) {
	q := r.db.WithContext(ctx).
		Table(model.Wallpaper{}.TableName()).
		Where("status = ?", model.WallpaperStatusActive)
	return listWallpaperIDs(q, "id", beforeID, limit, "list latest ids")
}

// ListActiveIDsAfter 命中覆盖索引 idx_status_id 升序游标扫描正常壁纸 ID（供 M6 低频对账滚动扫描）。
func (r *WallpaperRepository) ListActiveIDsAfter(ctx context.Context, afterID uint64, limit int) ([]uint64, error) {
	if limit <= 0 {
		return []uint64{}, nil
	}
	q := r.db.WithContext(ctx).
		Table(model.Wallpaper{}.TableName()).
		Where("status = ?", model.WallpaperStatusActive)
	if afterID > 0 {
		q = q.Where("id > ?", afterID)
	}
	ids := make([]uint64, 0, limit)
	if err := q.Select("id").Order("id ASC").Limit(limit).Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("wallpaper repo list active ids after: %w", err)
	}
	return ids, nil
}

// ListCategoryWallpaperIDs 命中覆盖索引 idx_status_category_id 倒序游标查询指定分类下的正常壁纸 ID。
func (r *WallpaperRepository) ListCategoryWallpaperIDs(ctx context.Context, categoryID, beforeID uint64, limit int) ([]uint64, error) {
	q := r.db.WithContext(ctx).
		Table(model.Wallpaper{}.TableName()).
		Where("status = ? AND category_id = ?", model.WallpaperStatusActive, categoryID)
	return listWallpaperIDs(q, "id", beforeID, limit, "list category wallpaper ids")
}

// ListCategoriesWallpaperIDs 倒序游标查询多个分类（大类 + 其下所有小类）下的正常壁纸 ID。
func (r *WallpaperRepository) ListCategoriesWallpaperIDs(ctx context.Context, categoryIDs []uint64, beforeID uint64, limit int) ([]uint64, error) {
	if len(categoryIDs) == 0 || limit <= 0 {
		return []uint64{}, nil
	}
	if len(categoryIDs) == 1 {
		return r.ListCategoryWallpaperIDs(ctx, categoryIDs[0], beforeID, limit)
	}
	q := r.db.WithContext(ctx).
		Table(model.Wallpaper{}.TableName()).
		Where("status = ? AND category_id IN ?", model.WallpaperStatusActive, categoryIDs)
	return listWallpaperIDs(q, "id", beforeID, limit, "list categories wallpaper ids")
}

// ListHotIDsByCategories 按互动加权热度降序返回指定分类集合内的 TopN 壁纸 ID。
func (r *WallpaperRepository) ListHotIDsByCategories(ctx context.Context, categoryIDs []uint64, limit int) ([]uint64, error) {
	if len(categoryIDs) == 0 || limit <= 0 {
		return []uint64{}, nil
	}
	ids := make([]uint64, 0, limit)
	err := r.db.WithContext(ctx).
		Table(model.Wallpaper{}.TableName()+" AS w").
		Select("w.id").
		Joins("LEFT JOIN wallpaper_stats AS s ON s.wallpaper_id = w.id").
		Where("w.status = ? AND w.category_id IN ?", model.WallpaperStatusActive, categoryIDs).
		Order("(COALESCE(s.like_count, 0) * 18 + COALESCE(s.fav_count, 0) * 15 + COALESCE(s.download_count, 0) * 12 + COALESCE(s.comment_count, 0) * 10) DESC, w.id DESC").
		Limit(limit).
		Pluck("w.id", &ids).Error
	if err != nil {
		return nil, fmt.Errorf("wallpaper repo list hot ids by categories: %w", err)
	}
	return ids, nil
}

// ListUserWallpaperIDs 命中覆盖索引 idx_user_status_id 倒序游标查询创作者已发布的正常壁纸 ID。
func (r *WallpaperRepository) ListUserWallpaperIDs(ctx context.Context, userID, beforeID uint64, limit int) ([]uint64, error) {
	q := r.db.WithContext(ctx).
		Table(model.Wallpaper{}.TableName()).
		Where("user_id = ? AND status = ?", userID, model.WallpaperStatusActive)
	return listWallpaperIDs(q, "id", beforeID, limit, "list user wallpaper ids")
}

// ListAuthorsWallpaperIDs 命中覆盖索引 idx_user_status_id 倒序查询多作者最新正常壁纸 ID（用于收件箱懒重建与关注流冷补查）。
func (r *WallpaperRepository) ListAuthorsWallpaperIDs(ctx context.Context, authorIDs []uint64, beforeID uint64, limit int) ([]uint64, error) {
	if len(authorIDs) == 0 || limit <= 0 {
		return []uint64{}, nil
	}
	q := r.db.WithContext(ctx).
		Table(model.Wallpaper{}.TableName()).
		Where("user_id IN ? AND status = ?", authorIDs, model.WallpaperStatusActive)
	return listWallpaperIDs(q, "id", beforeID, limit, "list authors wallpaper ids")
}

// ListTagWallpaperIDs 命中 wallpaper_tags.idx_tag_wp 倒序有序驱动并过滤正常状态，返回指定标签下的壁纸 ID。
func (r *WallpaperRepository) ListTagWallpaperIDs(ctx context.Context, tagID, beforeID uint64, limit int) ([]uint64, error) {
	q := r.db.WithContext(ctx).
		Table(model.WallpaperTag{}.TableName()+" AS wt").
		Joins("JOIN "+model.Wallpaper{}.TableName()+" AS w ON w.id = wt.wallpaper_id").
		Where("wt.tag_id = ? AND w.status = ?", tagID, model.WallpaperStatusActive)
	return listWallpaperIDs(q, "wt.wallpaper_id", beforeID, limit, "list tag wallpaper ids")
}

// ListHotRankBatch 命中 idx_status_id 倒序分批扫描正常壁纸并主键点查 wallpaper_stats（零 filesort，供 M7 热门榜算分）。
func (r *WallpaperRepository) ListHotRankBatch(ctx context.Context, beforeID uint64, limit int) ([]model.HotRankCandidate, error) {
	if limit <= 0 {
		return []model.HotRankCandidate{}, nil
	}

	q := r.db.WithContext(ctx).
		Table(model.Wallpaper{}.TableName()+" AS w").
		Select("w.id, w.created_at, COALESCE(ws.like_count, 0) AS like_count, COALESCE(ws.fav_count, 0) AS fav_count, COALESCE(ws.download_count, 0) AS download_count, COALESCE(ws.comment_count, 0) AS comment_count").
		Joins("LEFT JOIN "+model.WallpaperStats{}.TableName()+" AS ws ON ws.wallpaper_id = w.id").
		Where("w.status = ?", model.WallpaperStatusActive)
	if beforeID > 0 {
		q = q.Where("w.id < ?", beforeID)
	}

	rows := make([]model.HotRankCandidate, 0, limit)
	if err := q.Order("w.id DESC").Limit(limit).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("wallpaper repo list hot rank batch: %w", err)
	}
	return rows, nil
}

func listWallpaperIDs(q *gorm.DB, idCol string, beforeID uint64, limit int, op string) ([]uint64, error) {
	if limit <= 0 {
		return []uint64{}, nil
	}
	if beforeID > 0 {
		q = q.Where(idCol+" < ?", beforeID)
	}
	ids := make([]uint64, 0, limit)
	if err := q.Select(idCol).Order(idCol+" DESC").Limit(limit).Pluck(idCol, &ids).Error; err != nil {
		return nil, fmt.Errorf("wallpaper repo %s: %w", op, err)
	}
	return ids, nil
}
