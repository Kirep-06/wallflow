package repository

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"wallpaper-feed/internal/model"
)

var (
	ErrUserNotFound      = errors.New("repository: user not found")
	ErrDuplicateUsername = errors.New("repository: duplicate username")
)

// UserRepository 封装基于 GORM 的 users 表数据访问。
type UserRepository struct {
	db *gorm.DB
}

// NewUserRepository 构造基于 GORM 的用户仓储实例。
func NewUserRepository(db *gorm.DB) *UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) Create(ctx context.Context, user *model.User) error {
	if err := r.db.WithContext(ctx).Create(user).Error; err != nil {
		if isDuplicateKeyError(err) {
			return ErrDuplicateUsername
		}
		return fmt.Errorf("user repo create: %w", err)
	}
	return nil
}

func (r *UserRepository) FindAuthByUsername(ctx context.Context, username string) (*model.UserAuthRecord, error) {
	var rec model.UserAuthRecord
	err := r.db.WithContext(ctx).
		Table(model.User{}.TableName()).
		Select("id, username, password_hash, nickname, avatar_key, bio, created_at").
		Where("username = ?", username).
		Take(&rec).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("user repo find auth by username: %w", err)
	}
	return &rec, nil
}

func (r *UserRepository) FindProfileByID(ctx context.Context, id uint64) (*model.UserProfileCache, error) {
	var u model.User
	err := r.db.WithContext(ctx).
		Select("id, username, nickname, avatar_key, bio, created_at").
		Where("id = ?", id).
		Take(&u).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("user repo find profile by id: %w", err)
	}
	profile := u.ToProfileCache()
	return &profile, nil
}

// FindProfilesByIDs 按主键集合批量查询用户公开资料快照，用于三级缓存批量回源。
func (r *UserRepository) FindProfilesByIDs(ctx context.Context, ids []uint64) (map[uint64]model.UserProfileCache, error) {
	if len(ids) == 0 {
		return map[uint64]model.UserProfileCache{}, nil
	}
	var users []model.User
	if err := r.db.WithContext(ctx).
		Select("id, username, nickname, avatar_key, bio, created_at").
		Where("id IN ?", ids).
		Find(&users).Error; err != nil {
		return nil, fmt.Errorf("user repo find profiles by ids: %w", err)
	}
	result := make(map[uint64]model.UserProfileCache, len(users))
	for i := range users {
		result[users[i].ID] = users[i].ToProfileCache()
	}
	return result, nil
}

func (r *UserRepository) ExistsByID(ctx context.Context, id uint64) (bool, error) {
	var row struct {
		ID uint64
	}
	err := r.db.WithContext(ctx).
		Table(model.User{}.TableName()).
		Select("id").
		Where("id = ?", id).
		Take(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("user repo exists by id: %w", err)
	}
	return true, nil
}

func (r *UserRepository) UpdateProfileFields(ctx context.Context, id uint64, fields map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	if err := r.db.WithContext(ctx).
		Model(&model.User{}).
		Where("id = ?", id).
		Updates(fields).Error; err != nil {
		return fmt.Errorf("user repo update profile fields: %w", err)
	}
	return nil
}
