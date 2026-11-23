package repository

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"wallpaper-feed/internal/model"
)

// OutboxDeliveryOutcome 描述单条 outbox 事件经投递回调后的状态更新指令。
type OutboxDeliveryOutcome struct {
	ID          uint64
	Published   bool
	Dead        bool
	RetryCount  uint32
	NextRetryAt time.Time
	LastError   string
}

// OutboxRepository 封装 outbox_events 事务发件箱的抢占捞取、状态更新与历史清理。
type OutboxRepository struct {
	db *gorm.DB
}

// NewOutboxRepository 构造事务发件箱仓储。
func NewOutboxRepository(db *gorm.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// ProcessPendingBatch 在单次短事务内使用 FOR UPDATE SKIP LOCKED 捞取待投递事件并批量回写终态或退避状态。
func (r *OutboxRepository) ProcessPendingBatch(
	ctx context.Context,
	now time.Time,
	limit int,
	deliver func(batch []model.OutboxEvent) []OutboxDeliveryOutcome,
) (int, error) {
	if limit <= 0 || deliver == nil {
		return 0, nil
	}

	var fetched int
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var events []model.OutboxEvent
		if err := tx.
			Select("id, event_id, exchange_name, routing_key, payload, retry_count").
			Where("status = ? AND next_retry_at <= ?", model.OutboxStatusPending, now.UTC()).
			Order("next_retry_at ASC, id ASC").
			Limit(limit).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Find(&events).Error; err != nil {
			return fmt.Errorf("outbox repo lock pending batch: %w", err)
		}
		fetched = len(events)
		if fetched == 0 {
			return nil
		}

		outcomes := deliver(events)
		publishedIDs := make([]uint64, 0, len(outcomes))
		for _, out := range outcomes {
			if out.Published {
				publishedIDs = append(publishedIDs, out.ID)
				continue
			}
			status := model.OutboxStatusPending
			if out.Dead {
				status = model.OutboxStatusDead
			}
			updates := map[string]any{
				"status":        status,
				"retry_count":   out.RetryCount,
				"next_retry_at": out.NextRetryAt.UTC(),
				"last_error":    out.LastError,
			}
			if err := tx.Model(&model.OutboxEvent{}).Where("id = ?", out.ID).Updates(updates).Error; err != nil {
				return fmt.Errorf("outbox repo update failed event %d: %w", out.ID, err)
			}
		}

		if len(publishedIDs) > 0 {
			if err := tx.Model(&model.OutboxEvent{}).
				Where("id IN ?", publishedIDs).
				Updates(map[string]any{
					"status":     model.OutboxStatusPublished,
					"last_error": "",
				}).Error; err != nil {
				return fmt.Errorf("outbox repo mark published batch: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return fetched, nil
}

// DeletePublishedBefore 经 idx_status_updated 分批清理早于 cutoff 的已投递历史事件。
func (r *OutboxRepository) DeletePublishedBefore(ctx context.Context, cutoff time.Time, batchLimit int) (int64, error) {
	if batchLimit <= 0 {
		return 0, nil
	}
	res := r.db.WithContext(ctx).Exec(
		"DELETE FROM "+model.OutboxEvent{}.TableName()+" WHERE status = ? AND updated_at < ? LIMIT ?",
		model.OutboxStatusPublished,
		cutoff.UTC(),
		batchLimit,
	)
	if res.Error != nil {
		return 0, fmt.Errorf("outbox repo delete published before: %w", res.Error)
	}
	return res.RowsAffected, nil
}
