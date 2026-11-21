// Package service 是业务逻辑层。
package service

import (
	"context"
	"time"

	"wallpaper-feed/internal/model"
)

// PostStore 是帖子在 MySQL 中的数据访问抽象（便于单元测试替换）。
type PostStore interface {
	Create(ctx context.Context, p *model.Post) error
	CreateWithOutbox(ctx context.Context, p *model.Post, buildEvent func(post *model.Post) (*model.OutboxEvent, error)) error
	ListIDsBefore(ctx context.Context, beforeID uint64, limit int) ([]uint64, error)
	ListUserIDsBefore(ctx context.Context, userID, beforeID uint64, limit int) ([]uint64, error)
	ListNewerIDs(ctx context.Context, afterID uint64, limit int) ([]uint64, error)
	ListAuthorsIDsBefore(ctx context.Context, authorIDs []uint64, beforeID uint64, limit int, chunkSize int) ([]uint64, error)
	ListAuthorsNewerIDs(ctx context.Context, authorIDs []uint64, afterID uint64, limit int, chunkSize int) ([]uint64, error)
	GetByIDs(ctx context.Context, ids []uint64) ([]*model.Post, error)
}

// UserStore 是用户在 MySQL 中的数据访问抽象。
type UserStore interface {
	Create(ctx context.Context, u *model.User) error
	GetAuthByUsername(ctx context.Context, username string) (*model.UserAuthRecord, error)
	GetStatusByID(ctx context.Context, id uint64) (*model.UserStatusRecord, error)
	GetPublicByID(ctx context.Context, id uint64) (*model.UserPublic, error)
	GetPublicByIDs(ctx context.Context, ids []uint64) ([]*model.UserPublic, error)
	UpdateFields(ctx context.Context, id uint64, fields map[string]any) error
}

// FollowStore 是关注关系与计数在 MySQL 中的数据访问抽象。
type FollowStore interface {
	Follow(ctx context.Context, followerID, followeeID uint64, maxFollowing, bigVThreshold int, now time.Time) (model.FollowMutationResult, error)
	Unfollow(ctx context.Context, followerID, followeeID uint64, now time.Time) (model.FollowMutationResult, error)
	Exists(ctx context.Context, followerID, followeeID uint64) (bool, error)
	BatchIsFollowing(ctx context.Context, followerID uint64, followeeIDs []uint64) (map[uint64]bool, error)
	GetStatsByUserIDs(ctx context.Context, userIDs []uint64) (map[uint64]model.FollowStats, error)
	GetStats(ctx context.Context, userID uint64) (model.FollowStats, error)
	ListBigVIDs(ctx context.Context) ([]uint64, error)
	ListFollowingBefore(ctx context.Context, followerID, beforeID uint64, limit int) ([]model.FollowEdge, error)
	ListFollowersBefore(ctx context.Context, followeeID, beforeID uint64, limit int) ([]model.FollowEdge, error)
	ListFollowerIDsBefore(ctx context.Context, followeeID, cursorID uint64, limit int) ([]uint64, uint64, error)
	ListAllFolloweeIDs(ctx context.Context, followerID uint64) ([]uint64, error)
}
