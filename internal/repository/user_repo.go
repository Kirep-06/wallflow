package repository

import (
	"context"
	"errors"
	"fmt"

	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	"wallpaper-feed/internal/model"
)

var (
	ErrNotFound          = errors.New("record not found")
	ErrDuplicateUsername = errors.New("duplicate username")
)

type UserRepo struct {
	db *gorm.DB
}

func NewUserRepo(db *gorm.DB) *UserRepo { return &UserRepo{db: db} }

// Create 写入新用户，将 MySQL 唯一索引冲突（1062）映射为 ErrDuplicateUsername。
func (r *UserRepo) Create(ctx context.Context, u *model.User) error {
	if err := r.db.WithContext(ctx).Create(u).Error; err != nil {
		if isMySQLDuplicate(err) {
			return ErrDuplicateUsername
		}
		return fmt.Errorf("insert user: %w", err)
	}
	return nil
}

// GetAuthByUsername 仅用于密码登录，按用户名查询认证字段（含 password_hash）。
func (r *UserRepo) GetAuthByUsername(ctx context.Context, username string) (*model.UserAuthRecord, error) {
	var rec model.UserAuthRecord
	err := r.db.WithContext(ctx).
		Model(&model.User{}).
		Select("id", "username", "password_hash", "nickname", "avatar_key", "status").
		Where("username = ?", username).
		First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get auth user by username: %w", err)
	}
	return &rec, nil
}

// GetStatusByID 用于刷新令牌时校验用户存在性与封禁状态（不含 password_hash）。
func (r *UserRepo) GetStatusByID(ctx context.Context, id uint64) (*model.UserStatusRecord, error) {
	var rec model.UserStatusRecord
	err := r.db.WithContext(ctx).
		Model(&model.User{}).
		Select("id", "username", "nickname", "avatar_key", "status").
		Where("id = ?", id).
		First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get user status by id %d: %w", id, err)
	}
	return &rec, nil
}

// GetPublicByID 按主键查询用户公开资料（不含 password_hash）。
func (r *UserRepo) GetPublicByID(ctx context.Context, id uint64) (*model.UserPublic, error) {
	var rec model.UserPublic
	err := r.db.WithContext(ctx).
		Model(&model.User{}).
		Select("id", "username", "nickname", "avatar_key", "bio", "created_at").
		Where("id = ?", id).
		First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get public user by id %d: %w", id, err)
	}
	return &rec, nil
}

// GetPublicByIDs 批量查询用户公开资料（不含 password_hash）。
func (r *UserRepo) GetPublicByIDs(ctx context.Context, ids []uint64) ([]*model.UserPublic, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var users []*model.UserPublic
	err := r.db.WithContext(ctx).
		Model(&model.User{}).
		Select("id", "username", "nickname", "avatar_key", "bio", "created_at").
		Where("id IN ?", ids).
		Find(&users).Error
	if err != nil {
		return nil, fmt.Errorf("get public users by ids: %w", err)
	}
	return users, nil
}

// UpdateFields 执行字段级 UPDATE，只更新本次提供的字段与 updated_at。
func (r *UserRepo) UpdateFields(ctx context.Context, id uint64, fields map[string]any) error {
	if err := r.db.WithContext(ctx).Model(&model.User{}).Where("id = ?", id).Updates(fields).Error; err != nil {
		return fmt.Errorf("update user %d: %w", id, err)
	}
	return nil
}

func isMySQLDuplicate(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	var me *mysqldriver.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}
