package repository

import (
	"context"
	"fmt"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"

	"wallpaper-feed/internal/model"
)

const maxOutboxLastErrorBytes = 255

// OutboxRepo 管理 MySQL outbox_events 本地事务消息表。
type OutboxRepo struct {
	db *gorm.DB
}

func NewOutboxRepo(db *gorm.DB) *OutboxRepo { return &OutboxRepo{db: db} }

// CreateInTx 在调用方既有事务内写入一条待投递事件。
func (r *OutboxRepo) CreateInTx(tx *gorm.DB, ev *model.OutboxEvent) error {
	if err := tx.Create(ev).Error; err != nil {
		return fmt.Errorf("insert outbox event %s: %w", ev.EventID, err)
	}
	return nil
}

// ClaimBatch 在短事务内通过 FOR UPDATE SKIP LOCKED 捞取一批到期事件并续延租约。
// 事务仅做本地数据库读写，绝不掺杂网络 I/O。
func (r *OutboxRepo) ClaimBatch(ctx context.Context, limit int, lease time.Duration, now time.Time) ([]*model.OutboxEvent, error) {
	if limit <= 0 {
		return nil, nil
	}
	var events []*model.OutboxEvent
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Raw(
			`SELECT id, event_id, exchange_name, routing_key, payload, status, retry_count, next_retry_at, last_error, created_at, updated_at
			 FROM outbox_events
			 WHERE status = ? AND next_retry_at <= ?
			 ORDER BY id ASC
			 LIMIT ?
			 FOR UPDATE SKIP LOCKED`,
			model.OutboxStatusPending, now, limit,
		).Scan(&events).Error; err != nil {
			return fmt.Errorf("claim outbox batch: %w", err)
		}
		if len(events) == 0 {
			return nil
		}
		ids := make([]uint64, len(events))
		for i, ev := range events {
			ids[i] = ev.ID
		}
		leaseUntil := now.Add(lease)
		if err := tx.Model(&model.OutboxEvent{}).
			Where("id IN ?", ids).
			Updates(map[string]any{
				"next_retry_at": leaseUntil,
				"updated_at":    now,
			}).Error; err != nil {
			return fmt.Errorf("extend outbox lease: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

// MarkSent 批量将已收到 Publisher Confirm 的事件置为已投递。
func (r *OutboxRepo) MarkSent(ctx context.Context, ids []uint64, now time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	if err := r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id IN ? AND status = ?", ids, model.OutboxStatusPending).
		Updates(map[string]any{
			"status":     model.OutboxStatusSent,
			"updated_at": now,
		}).Error; err != nil {
		return fmt.Errorf("mark outbox events sent: %w", err)
	}
	return nil
}

// MarkFailed 记录单条事件投递失败：递增 retry_count 并更新退避时间；超过上限时置为死信 status=2。
func (r *OutboxRepo) MarkFailed(ctx context.Context, id uint64, retryCount uint16, nextRetryAt time.Time, dead bool, lastErr string, now time.Time) error {
	status := model.OutboxStatusPending
	if dead {
		status = model.OutboxStatusDead
	}
	trimmed := truncateUTF8Bytes(lastErr, maxOutboxLastErrorBytes)
	if err := r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":        status,
			"retry_count":   retryCount,
			"next_retry_at": nextRetryAt,
			"last_error":    trimmed,
			"updated_at":    now,
		}).Error; err != nil {
		return fmt.Errorf("mark outbox event %d failed: %w", id, err)
	}
	return nil
}

// ReleaseLease 在因 AMQP 连接级断开提前中止本批投递时，缩短尚未尝试事件的租约而不递增 retry_count。
func (r *OutboxRepo) ReleaseLease(ctx context.Context, ids []uint64, nextRetryAt, now time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	if err := r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id IN ? AND status = ?", ids, model.OutboxStatusPending).
		Updates(map[string]any{
			"next_retry_at": nextRetryAt,
			"updated_at":    now,
		}).Error; err != nil {
		return fmt.Errorf("release outbox lease: %w", err)
	}
	return nil
}

// CleanupSentBefore 分批删除指定截止时间之前的已投递记录，控制每批大小与批数上限防止长事务。
func (r *OutboxRepo) CleanupSentBefore(ctx context.Context, cutoff time.Time, batchLimit, maxBatches int) (int64, error) {
	if batchLimit <= 0 {
		batchLimit = 1000
	}
	if maxBatches <= 0 {
		maxBatches = 10
	}
	var total int64
	for range maxBatches {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		res := r.db.WithContext(ctx).Exec(
			"DELETE FROM outbox_events WHERE status = ? AND updated_at < ? LIMIT ?",
			model.OutboxStatusSent, cutoff, batchLimit,
		)
		if res.Error != nil {
			return total, fmt.Errorf("cleanup outbox events: %w", res.Error)
		}
		total += res.RowsAffected
		if res.RowsAffected < int64(batchLimit) {
			break
		}
	}
	return total, nil
}

// truncateUTF8Bytes 将字符串按字节长度安全截断，保证不会把多字节 UTF-8 字符截断成非法序列。
func truncateUTF8Bytes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	n := 0
	for i := range s {
		if i > maxBytes {
			break
		}
		n = i
	}
	// 如果最后一个起始位置后的 rune 整体超出上限，取 n；检查合法性。
	if n == 0 && maxBytes > 0 {
		// 单个 rune 超过 maxBytes 的极端情况
		r, size := utf8.DecodeRuneInString(s)
		if r != utf8.RuneError && size <= maxBytes {
			return s[:size]
		}
		return ""
	}
	return s[:n]
}
