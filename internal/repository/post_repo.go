// Package repository 是 MySQL / Redis 数据访问层。
package repository

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"gorm.io/gorm"

	"wallpaper-feed/internal/model"
)

const defaultAuthorInChunkSize = 200

type PostRepo struct {
	db *gorm.DB
}

func NewPostRepo(db *gorm.DB) *PostRepo { return &PostRepo{db: db} }

// Create 在一个事务中写入帖子与图片。
func (r *PostRepo) Create(ctx context.Context, p *model.Post) error {
	return r.CreateWithOutbox(ctx, p, nil)
}

// CreateWithOutbox 在同一个本地事务内原子写入帖子、图片与可选的 outbox_events 记录。
// buildEvent 在帖子插入回填自增 ID 后调用，仅执行纯内存序列化，绝不进行网络 I/O。
func (r *PostRepo) CreateWithOutbox(ctx context.Context, p *model.Post, buildEvent func(post *model.Post) (*model.OutboxEvent, error)) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(p).Error; err != nil { // GORM 会级联写入 p.Images
			return fmt.Errorf("insert post: %w", err)
		}
		if buildEvent != nil {
			ev, err := buildEvent(p)
			if err != nil {
				return fmt.Errorf("build outbox event for post %d: %w", p.ID, err)
			}
			if ev != nil {
				if err := tx.Create(ev).Error; err != nil {
					return fmt.Errorf("insert outbox event %s: %w", ev.EventID, err)
				}
			}
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

// ListUserIDsBefore 按 ID 倒序返回指定用户的正常帖子 ID（命中 idx_user_id）；beforeID 为 0 表示首页。
func (r *PostRepo) ListUserIDsBefore(ctx context.Context, userID, beforeID uint64, limit int) ([]uint64, error) {
	q := r.db.WithContext(ctx).Model(&model.Post{}).
		Where("user_id = ? AND status = ?", userID, model.PostStatusNormal)
	if beforeID > 0 {
		q = q.Where("id < ?", beforeID)
	}
	var ids []uint64
	if err := q.Order("id DESC").Limit(limit).Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("list user %d post ids before %d: %w", userID, beforeID, err)
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

// ListAuthorsIDsBefore 按 ID 倒序查询指定作者集合中 id < beforeID 的正常帖子 ID（命中 idx_user_status_id 覆盖索引，零回表）。
// beforeID 为 0 表示不设上界；单作者走等值查询无 filesort，多作者按 chunkSize 分块 IN 查询后在内存中去重倒序截断。
func (r *PostRepo) ListAuthorsIDsBefore(ctx context.Context, authorIDs []uint64, beforeID uint64, limit int, chunkSize int) ([]uint64, error) {
	if len(authorIDs) == 0 || limit <= 0 {
		return []uint64{}, nil
	}
	if len(authorIDs) == 1 {
		q := r.db.WithContext(ctx).Model(&model.Post{}).
			Where("user_id = ? AND status = ?", authorIDs[0], model.PostStatusNormal)
		if beforeID > 0 {
			q = q.Where("id < ?", beforeID)
		}
		var ids []uint64
		if err := q.Order("id DESC").Limit(limit).Pluck("id", &ids).Error; err != nil {
			return nil, fmt.Errorf("list author %d post ids before %d: %w", authorIDs[0], beforeID, err)
		}
		return ids, nil
	}
	if chunkSize <= 0 {
		chunkSize = defaultAuthorInChunkSize
	}

	var merged []uint64
	chunks := 0
	for start := 0; start < len(authorIDs); start += chunkSize {
		end := min(start+chunkSize, len(authorIDs))
		chunk := authorIDs[start:end]
		q := r.db.WithContext(ctx).Model(&model.Post{}).
			Where("user_id IN ? AND status = ?", chunk, model.PostStatusNormal)
		if beforeID > 0 {
			q = q.Where("id < ?", beforeID)
		}
		var ids []uint64
		if err := q.Order("id DESC").Limit(limit).Pluck("id", &ids).Error; err != nil {
			return nil, fmt.Errorf("list authors post ids before %d (chunk %d): %w", beforeID, chunks, err)
		}
		if chunks == 0 && end == len(authorIDs) {
			return ids, nil
		}
		merged = append(merged, ids...)
		chunks++
	}
	return dedupSortDescLimit(merged, limit), nil
}

// ListAuthorsNewerIDs 按 ID 倒序查询指定作者集合中 id > afterID 的最新正常帖子 ID，用于收件箱/发件箱重建后的两段补漏。
func (r *PostRepo) ListAuthorsNewerIDs(ctx context.Context, authorIDs []uint64, afterID uint64, limit int, chunkSize int) ([]uint64, error) {
	if len(authorIDs) == 0 || limit <= 0 {
		return []uint64{}, nil
	}
	if len(authorIDs) == 1 {
		var ids []uint64
		if err := r.db.WithContext(ctx).Model(&model.Post{}).
			Where("user_id = ? AND status = ? AND id > ?", authorIDs[0], model.PostStatusNormal, afterID).
			Order("id DESC").Limit(limit).Pluck("id", &ids).Error; err != nil {
			return nil, fmt.Errorf("list author %d post ids after %d: %w", authorIDs[0], afterID, err)
		}
		return ids, nil
	}
	if chunkSize <= 0 {
		chunkSize = defaultAuthorInChunkSize
	}

	var merged []uint64
	chunks := 0
	for start := 0; start < len(authorIDs); start += chunkSize {
		end := min(start+chunkSize, len(authorIDs))
		chunk := authorIDs[start:end]
		var ids []uint64
		if err := r.db.WithContext(ctx).Model(&model.Post{}).
			Where("user_id IN ? AND status = ? AND id > ?", chunk, model.PostStatusNormal, afterID).
			Order("id DESC").Limit(limit).Pluck("id", &ids).Error; err != nil {
			return nil, fmt.Errorf("list authors post ids after %d (chunk %d): %w", afterID, chunks, err)
		}
		if chunks == 0 && end == len(authorIDs) {
			return ids, nil
		}
		merged = append(merged, ids...)
		chunks++
	}
	return dedupSortDescLimit(merged, limit), nil
}

func dedupSortDescLimit(ids []uint64, limit int) []uint64 {
	if len(ids) == 0 {
		return []uint64{}
	}
	slices.SortFunc(ids, func(a, b uint64) int { return cmp.Compare(b, a) })
	out := make([]uint64, 0, min(len(ids), limit))
	var prev uint64
	for _, id := range ids {
		if id == 0 || id == prev {
			continue
		}
		prev = id
		out = append(out, id)
		if len(out) >= limit {
			break
		}
	}
	return out
}
