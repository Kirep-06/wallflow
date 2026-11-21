// Package repository 是 MySQL / Redis 数据访问层。
package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"wallpaper-feed/internal/model"
)

type PostRepo struct {
	db *gorm.DB
}

func NewPostRepo(db *gorm.DB) *PostRepo { return &PostRepo{db: db} }

// Create 在一个事务中写入帖子与图片。
func (r *PostRepo) Create(ctx context.Context, p *model.Post) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(p).Error; err != nil { // GORM 会级联写入 p.Images
			return fmt.Errorf("insert post: %w", err)
		}
		return nil
	})
}

// ListIDsBefore 按 ID 倒序返回 id < beforeID 的正常帖子；beforeID 为 0 表示不设上界。
func (r *PostRepo) ListIDsBefore(ctx context.Context, beforeID uint64, limit int) ([]uint64, error) {
	q := r.db.WithContext(ctx).Model(&model.Post{}).Where("status = ?", model.PostStatusNormal)
	if beforeID > 0 {
		q = q.Where("id < ?", beforeID)
	}
	var ids []uint64
	if err := q.Order("id DESC").Limit(limit).Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("list post ids before %d: %w", beforeID, err)
	}
	return ids, nil
}

// ListNewerIDs 按 ID 倒序返回 id > afterID 的最新正常帖子，最多 limit 条。
func (r *PostRepo) ListNewerIDs(ctx context.Context, afterID uint64, limit int) ([]uint64, error) {
	var ids []uint64
	err := r.db.WithContext(ctx).Model(&model.Post{}).
		Where("status = ? AND id > ?", model.PostStatusNormal, afterID).
		Order("id DESC").Limit(limit).Pluck("id", &ids).Error
	if err != nil {
		return nil, fmt.Errorf("list post ids after %d: %w", afterID, err)
	}
	return ids, nil
}

// GetByIDs 批量查询帖子及其图片（图片按 sort_no 升序）；不过滤状态，由上层决定。
func (r *PostRepo) GetByIDs(ctx context.Context, ids []uint64) ([]*model.Post, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var posts []*model.Post
	err := r.db.WithContext(ctx).
		Preload("Images", func(db *gorm.DB) *gorm.DB { return db.Order("sort_no ASC") }).
		Where("id IN ?", ids).Find(&posts).Error
	if err != nil {
		return nil, fmt.Errorf("get posts by ids: %w", err)
	}
	return posts, nil
}
