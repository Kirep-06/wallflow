package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/mq"
	"wallpaper-feed/internal/repository"
)

// FollowFanoutStore 抽象扇出消费所需的关注关系查询能力。
type FollowFanoutStore interface {
	GetStats(ctx context.Context, userID uint64) (model.FollowStats, error)
	ListFollowerIDsBefore(ctx context.Context, followeeID, cursorID uint64, limit int) ([]uint64, uint64, error)
}

// FollowingFanoutConsumer 消费 post.created 事件并执行推拉结合扇出：
//   - 大 V（is_bigv = 1）：仅条件追加至其发件箱 feed:outbox:{author_id}，不向粉丝写扩散；
//   - 普通作者（is_bigv = 0）：按游标分批扫描粉丝并 Pipeline 条件写入活跃粉丝收件箱 feed:inbox:{follower_id}。
type FollowingFanoutConsumer struct {
	mqConsumer *mq.Consumer
	follows    FollowFanoutStore
	feedRepo   *repository.FollowingFeedRepo
	batchSize  int
}

func NewFollowingFanoutConsumer(
	mqConsumer *mq.Consumer,
	follows FollowFanoutStore,
	feedRepo *repository.FollowingFeedRepo,
	batchSize int,
) *FollowingFanoutConsumer {
	if batchSize <= 0 {
		batchSize = 500
	}
	return &FollowingFanoutConsumer{
		mqConsumer: mqConsumer,
		follows:    follows,
		feedRepo:   feedRepo,
		batchSize:  batchSize,
	}
}

// Run 启动扇出消费循环，直到 ctx 取消并优雅排空在途消息。
func (c *FollowingFanoutConsumer) Run(ctx context.Context) error {
	return c.mqConsumer.Run(ctx, c.handleMessage)
}

func (c *FollowingFanoutConsumer) handleMessage(ctx context.Context, body []byte) error {
	var ev model.PostCreatedEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return fmt.Errorf("%w: unmarshal post.created event: %v", mq.ErrPermanent, err)
	}
	if ev.PostID == 0 || ev.AuthorID == 0 {
		return fmt.Errorf("%w: invalid post.created payload post_id=%d author_id=%d", mq.ErrPermanent, ev.PostID, ev.AuthorID)
	}

	// 实时查询作者当前是否大 V，保证作者在消息排队期间晋升大 V 时的路由决策最新。
	stats, err := c.follows.GetStats(ctx, ev.AuthorID)
	if err != nil {
		return fmt.Errorf("load author %d follow stats: %w", ev.AuthorID, err)
	}

	if stats.IsBigV {
		written, err := c.feedRepo.AddToOutboxIfExists(ctx, ev.AuthorID, ev.PostID)
		if err != nil {
			return fmt.Errorf("append post %d to bigv outbox %d: %w", ev.PostID, ev.AuthorID, err)
		}
		slog.DebugContext(ctx, "bigv post fanout skipped, outbox updated",
			"post_id", ev.PostID, "author_id", ev.AuthorID, "outbox_exists", written)
		return nil
	}

	var cursorID uint64
	batches := 0
	followers := 0
	for {
		followerIDs, nextCursor, err := c.follows.ListFollowerIDsBefore(ctx, ev.AuthorID, cursorID, c.batchSize)
		if err != nil {
			return fmt.Errorf("list followers of author %d cursor %d: %w", ev.AuthorID, cursorID, err)
		}
		if len(followerIDs) == 0 {
			break
		}
		if err := c.feedRepo.BatchAddToInboxesIfExists(ctx, followerIDs, ev.PostID); err != nil {
			return fmt.Errorf("fanout post %d batch %d: %w", ev.PostID, batches, err)
		}
		batches++
		followers += len(followerIDs)
		if len(followerIDs) < c.batchSize || nextCursor == 0 {
			break
		}
		cursorID = nextCursor
	}
	slog.DebugContext(ctx, "normal post fanout finished",
		"post_id", ev.PostID, "author_id", ev.AuthorID, "batches", batches, "scanned_followers", followers)
	return nil
}
