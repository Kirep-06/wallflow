package repository

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"wallpaper-feed/internal/model"
)

var (
	ErrTagNotFound      = errors.New("repository: tag not found")
	ErrTagNameDuplicate = errors.New("repository: tag name already exists")
)

// MetaRepository 封装标签字典的 MySQL 直查与管理写入访问。
type MetaRepository struct {
	db *gorm.DB
}

// NewMetaRepository 构造标签仓储实例。
func NewMetaRepository(db *gorm.DB) *MetaRepository {
	return &MetaRepository{db: db}
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

// FindTagByID 按主键查询单个标签。
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

// CreateTag 新增标签，重名返回 ErrTagNameDuplicate。
func (r *MetaRepository) CreateTag(ctx context.Context, tag *model.Tag) error {
	if err := r.db.WithContext(ctx).Create(tag).Error; err != nil {
		if isDuplicateKeyError(err) {
			return ErrTagNameDuplicate
		}
		return fmt.Errorf("meta repo create tag: %w", err)
	}
	return nil
}

// UpdateTag 更新指定标签字段，重名返回 ErrTagNameDuplicate。
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
