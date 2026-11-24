package repository

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"wallpaper-feed/internal/model"
)

var (
	ErrCategoryNotFound      = errors.New("repository: category not found")
	ErrCategorySlugDuplicate = errors.New("repository: category slug already exists")
	ErrCategoryHasWallpapers = errors.New("repository: category still has wallpapers")
	ErrTagNotFound           = errors.New("repository: tag not found")
	ErrTagNameDuplicate      = errors.New("repository: tag name already exists")
)

// MetaRepository 封装分类与标签字典的 MySQL 直查与管理写入访问。
type MetaRepository struct {
	db *gorm.DB
}

// NewMetaRepository 构造分类与标签仓储实例。
func NewMetaRepository(db *gorm.DB) *MetaRepository {
	return &MetaRepository{db: db}
}

// ListCategories 按 sort_no 升序、id 升序返回全部分类。
func (r *MetaRepository) ListCategories(ctx context.Context) ([]model.Category, error) {
	var items []model.Category
	if err := r.db.WithContext(ctx).
		Select("id, slug, name, sort_no, icon_key, created_at").
		Order("sort_no ASC, id ASC").
		Find(&items).Error; err != nil {
		return nil, fmt.Errorf("meta repo list categories: %w", err)
	}
	return items, nil
}

// CountWallpapersByCategory 统计各分类下正常壁纸数量。
func (r *MetaRepository) CountWallpapersByCategory(ctx context.Context) (map[uint64]uint64, error) {
	type row struct {
		CategoryID uint64
		Cnt        uint64
	}
	var rows []row
	if err := r.db.WithContext(ctx).
		Model(&model.Wallpaper{}).
		Select("category_id, COUNT(*) AS cnt").
		Where("status = ?", model.WallpaperStatusActive).
		Group("category_id").
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("meta repo count wallpapers by category: %w", err)
	}
	counts := make(map[uint64]uint64, len(rows))
	for _, item := range rows {
		counts[item.CategoryID] = item.Cnt
	}
	return counts, nil
}

// FindCategoryByID 按主键查询单个分类。
func (r *MetaRepository) FindCategoryByID(ctx context.Context, id uint64) (*model.Category, error) {
	var cat model.Category
	err := r.db.WithContext(ctx).
		Select("id, slug, name, sort_no, icon_key, created_at").
		Where("id = ?", id).
		Take(&cat).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCategoryNotFound
		}
		return nil, fmt.Errorf("meta repo find category by id: %w", err)
	}
	return &cat, nil
}

// CreateCategory 新增分类，slug 冲突返回 ErrCategorySlugDuplicate。
func (r *MetaRepository) CreateCategory(ctx context.Context, cat *model.Category) error {
	if err := r.db.WithContext(ctx).Create(cat).Error; err != nil {
		if isDuplicateKeyError(err) {
			return ErrCategorySlugDuplicate
		}
		return fmt.Errorf("meta repo create category: %w", err)
	}
	return nil
}

// UpdateCategory 更新指定分类字段，slug 冲突返回 ErrCategorySlugDuplicate。
func (r *MetaRepository) UpdateCategory(ctx context.Context, id uint64, fields map[string]any) (*model.Category, error) {
	if len(fields) > 0 {
		res := r.db.WithContext(ctx).Model(&model.Category{}).Where("id = ?", id).Updates(fields)
		if res.Error != nil {
			if isDuplicateKeyError(res.Error) {
				return nil, ErrCategorySlugDuplicate
			}
			return nil, fmt.Errorf("meta repo update category: %w", res.Error)
		}
	}
	return r.FindCategoryByID(ctx, id)
}

// DeleteCategoryWithMigrate 安全删除分类：若存在壁纸且 migrateToID == 0 则返回 ErrCategoryHasWallpapers；
// 若指定了合法 migrateToID，则同事务迁移所有壁纸后再删除分类，返回被迁移的壁纸 ID 列表（用于清理缓存）。
func (r *MetaRepository) DeleteCategoryWithMigrate(ctx context.Context, id uint64, migrateToID uint64) ([]uint64, error) {
	var affectedIDs []uint64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var cat model.Category
		if err := tx.Where("id = ?", id).Take(&cat).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrCategoryNotFound
			}
			return fmt.Errorf("find category: %w", err)
		}

		var wpCount int64
		if err := tx.Model(&model.Wallpaper{}).
			Where("category_id = ? AND status = ?", id, model.WallpaperStatusActive).
			Count(&wpCount).Error; err != nil {
			return fmt.Errorf("count category wallpapers: %w", err)
		}

		if wpCount > 0 {
			if migrateToID == 0 || migrateToID == id {
				return ErrCategoryHasWallpapers
			}
			var target model.Category
			if err := tx.Where("id = ?", migrateToID).Take(&target).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrCategoryNotFound
				}
				return fmt.Errorf("find target category: %w", err)
			}

			if err := tx.Model(&model.Wallpaper{}).
				Where("category_id = ?", id).
				Pluck("id", &affectedIDs).Error; err != nil {
				return fmt.Errorf("pluck affected wallpapers: %w", err)
			}
			if err := tx.Model(&model.Wallpaper{}).
				Where("category_id = ?", id).
				Update("category_id", migrateToID).Error; err != nil {
				return fmt.Errorf("migrate wallpapers: %w", err)
			}
		} else {
			// 若存在已软删壁纸指向该分类且提供了迁移目标，一并迁移保持引用整洁
			if migrateToID > 0 && migrateToID != id {
				_ = tx.Model(&model.Wallpaper{}).Where("category_id = ?", id).Update("category_id", migrateToID).Error
			}
		}

		if err := tx.Delete(&model.Category{}, id).Error; err != nil {
			return fmt.Errorf("delete category: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return affectedIDs, nil
}

// ListHotTags 按 use_count 降序、id 升序返回热门标签。
func (r *MetaRepository) ListHotTags(ctx context.Context, limit int) ([]model.Tag, error) {
	var items []model.Tag
	if err := r.db.WithContext(ctx).
		Select("id, name, use_count, icon_key, created_at").
		Order("use_count DESC, id ASC").
		Limit(limit).
		Find(&items).Error; err != nil {
		return nil, fmt.Errorf("meta repo list hot tags: %w", err)
	}
	return items, nil
}

// SearchTagsByPrefix 按转义后的前缀模式检索标签，按热度降序、id 升序返回。
func (r *MetaRepository) SearchTagsByPrefix(ctx context.Context, likePattern string, limit int) ([]model.Tag, error) {
	var items []model.Tag
	if err := r.db.WithContext(ctx).
		Select("id, name, use_count, icon_key, created_at").
		Where(`name LIKE ? ESCAPE '\\'`, likePattern).
		Order("use_count DESC, id ASC").
		Limit(limit).
		Find(&items).Error; err != nil {
		return nil, fmt.Errorf("meta repo search tags by prefix: %w", err)
	}
	return items, nil
}

// FindTagByID 按主键查询单个话题/标签。
func (r *MetaRepository) FindTagByID(ctx context.Context, id uint64) (*model.Tag, error) {
	var tag model.Tag
	err := r.db.WithContext(ctx).
		Select("id, name, use_count, icon_key, created_at").
		Where("id = ?", id).
		Take(&tag).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrTagNotFound
		}
		return nil, fmt.Errorf("meta repo find tag by id: %w", err)
	}
	return &tag, nil
}

// CreateTag 新增话题/标签，重名返回 ErrTagNameDuplicate。
func (r *MetaRepository) CreateTag(ctx context.Context, tag *model.Tag) error {
	if err := r.db.WithContext(ctx).Create(tag).Error; err != nil {
		if isDuplicateKeyError(err) {
			return ErrTagNameDuplicate
		}
		return fmt.Errorf("meta repo create tag: %w", err)
	}
	return nil
}

// UpdateTag 更新指定话题/标签字段，重名返回 ErrTagNameDuplicate。
func (r *MetaRepository) UpdateTag(ctx context.Context, id uint64, fields map[string]any) (*model.Tag, error) {
	if len(fields) > 0 {
		res := r.db.WithContext(ctx).Model(&model.Tag{}).Where("id = ?", id).Updates(fields)
		if res.Error != nil {
			if isDuplicateKeyError(res.Error) {
				return nil, ErrTagNameDuplicate
			}
			return nil, fmt.Errorf("meta repo update tag: %w", res.Error)
		}
	}
	return r.FindTagByID(ctx, id)
}

// DeleteTag 同事务删除标签关联记录与标签主记录，返回受影响的壁纸 ID 列表（用于清理缓存）。
func (r *MetaRepository) DeleteTag(ctx context.Context, id uint64) ([]uint64, error) {
	var affectedIDs []uint64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var tag model.Tag
		if err := tx.Where("id = ?", id).Take(&tag).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrTagNotFound
			}
			return fmt.Errorf("find tag: %w", err)
		}
		if err := tx.Model(&model.WallpaperTag{}).
			Where("tag_id = ?", id).
			Pluck("wallpaper_id", &affectedIDs).Error; err != nil {
			return fmt.Errorf("pluck tag wallpapers: %w", err)
		}
		if err := tx.Where("tag_id = ?", id).Delete(&model.WallpaperTag{}).Error; err != nil {
			return fmt.Errorf("delete wallpaper_tags: %w", err)
		}
		if err := tx.Delete(&model.Tag{}, id).Error; err != nil {
			return fmt.Errorf("delete tag: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return affectedIDs, nil
}
