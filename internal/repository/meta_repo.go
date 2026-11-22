package repository

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"wallpaper-feed/internal/model"
)

var ErrCategoryNotFound = errors.New("repository: category not found")

// MetaRepository 封装分类与标签字典的 MySQL 直查访问。
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
		Select("id, slug, name, sort_no, created_at").
		Order("sort_no ASC, id ASC").
		Find(&items).Error; err != nil {
		return nil, fmt.Errorf("meta repo list categories: %w", err)
	}
	return items, nil
}

// FindCategoryByID 按主键查询单个分类。
func (r *MetaRepository) FindCategoryByID(ctx context.Context, id uint64) (*model.Category, error) {
	var cat model.Category
	err := r.db.WithContext(ctx).
		Select("id, slug, name, sort_no, created_at").
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

// ListHotTags 按 use_count 降序、id 升序返回热门标签。
func (r *MetaRepository) ListHotTags(ctx context.Context, limit int) ([]model.Tag, error) {
	var items []model.Tag
	if err := r.db.WithContext(ctx).
		Select("id, name, use_count, created_at").
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
		Select("id, name, use_count, created_at").
		Where(`name LIKE ? ESCAPE '\\'`, likePattern).
		Order("use_count DESC, id ASC").
		Limit(limit).
		Find(&items).Error; err != nil {
		return nil, fmt.Errorf("meta repo search tags by prefix: %w", err)
	}
	return items, nil
}
