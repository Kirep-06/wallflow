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

// CreateWithTags 在单次短事务内写入壁纸、按需建标签并递增标签计数（按 tag_id 升序防死锁）。
func (r *WallpaperRepository) CreateWithTags(ctx context.Context, wp *model.Wallpaper, tagNames []string) ([]model.Tag, error) {
	attached := make([]model.Tag, 0, len(tagNames))
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(wp).Error; err != nil {
			if isDuplicateKeyError(err) {
				return ErrDuplicateObjectKey
			}
			return fmt.Errorf("wallpaper repo create: %w", err)
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

		// M5: 同事务写入 outbox_events (wallpaper.published)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return attached, nil
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
