package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/mq"
	"wallpaper-feed/internal/repository"
)

const (
	fanoutConsumerTag   = "worker-following-fanout"
	fanoutPrefetch      = 8
	fanoutWorkers       = 4
	fanoutMaxRetries    = 5
	fanoutFollowerBatch = 500
)

// FollowingFanoutConsumer 消费 wallpaper.published 事件，执行普通作者写扩散或大 V 发件箱追加。
type FollowingFanoutConsumer struct {
	wallpaperRepo *repository.WallpaperRepository
	followRepo    *repository.FollowRepository
	followingFeed *cache.FollowingFeed
}

// NewFollowingFanoutConsumer 构造关注流推拉扇出消费者。
func NewFollowingFanoutConsumer(
	wallpaperRepo *repository.WallpaperRepository,
	followRepo *repository.FollowRepository,
	followingFeed *cache.FollowingFeed,
) *FollowingFanoutConsumer {
	return &FollowingFanoutConsumer{
		wallpaperRepo: wallpaperRepo,
		followRepo:    followRepo,
		followingFeed: followingFeed,
	}
}

// Run 在给定 mq.Client 上启动消费者并托管断连重建、三队列重试与停机优雅排空。
func (c *FollowingFanoutConsumer) Run(ctx context.Context, client *mq.Client) error {
	return client.Consume(ctx, mq.ConsumerOptions{
		Queue:           mq.QueueFollowingFanout,
		ConsumerTag:     fanoutConsumerTag,
		Prefetch:        fanoutPrefetch,
		Workers:         fanoutWorkers,
		MaxRetries:      fanoutMaxRetries,
		RetryRoutingKey: mq.RoutingKeyFollowingFanoutRetry,
	}, c.handleDelivery)
}

func (c *FollowingFanoutConsumer) handleDelivery(ctx context.Context, d mq.Delivery) error {
	var ev model.WallpaperPublishedEvent
	if err := json.Unmarshal(d.Body, &ev); err != nil {
		return mq.PermanentError(fmt.Errorf("unmarshal wallpaper published event: %w", err))
	}
	if ev.WallpaperID == 0 || ev.AuthorID == 0 {
		return mq.PermanentError(errors.New("wallpaper_id and author_id must be positive"))
	}

	// 二次核验壁纸状态，避免作者刚发完即软删导致向活跃粉丝收件箱写入幽灵 ID。
	active, err := c.wallpaperRepo.IsActive(ctx, ev.WallpaperID)
	if err != nil {
		return err
	}
	if !active {
		slog.InfoContext(ctx, "skip fanout for inactive wallpaper",
			slog.Uint64("wallpaper_id", ev.WallpaperID),
			slog.Uint64("author_id", ev.AuthorID),
		)
		return nil
	}

	// 直查 MySQL 权威 is_bigv，不受大 V 缓存对账窗口滞后影响。
	stats, err := c.followRepo.FindStatsByUserID(ctx, ev.AuthorID)
	if err != nil {
		return err
	}
	if stats.IsBigV {
		return c.followingFeed.PushOutboxIfExists(ctx, ev.AuthorID, ev.WallpaperID)
	}

	var afterID uint64
	followerIDs := make([]uint64, 0, fanoutFollowerBatch)
	for {
		edges, err := c.followRepo.ListFollowerIDsAfter(ctx, ev.AuthorID, afterID, fanoutFollowerBatch)
		if err != nil {
			return err
		}
		if len(edges) == 0 {
			return nil
		}

		followerIDs = followerIDs[:0]
		for _, e := range edges {
			followerIDs = append(followerIDs, e.PeerID)
		}
		if err := c.followingFeed.BatchPushInboxIfExists(ctx, followerIDs, ev.WallpaperID); err != nil {
			return err
		}

		if len(edges) < fanoutFollowerBatch {
			return nil
		}
		afterID = edges[len(edges)-1].ID
	}
}
