// Package service 是业务逻辑层。
package service

import (
	"context"

	"wallpaper-feed/internal/model"
)

// PostStore 是帖子在 MySQL 中的数据访问抽象（便于单元测试替换）。
type PostStore interface {
	Create(ctx context.Context, p *model.Post) error
	ListIDsBefore(ctx context.Context, beforeID uint64, limit int) ([]uint64, error)
	ListNewerIDs(ctx context.Context, afterID uint64, limit int) ([]uint64, error)
	GetByIDs(ctx context.Context, ids []uint64) ([]*model.Post, error)
}
