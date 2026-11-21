package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"wallpaper-feed/internal/model"
)

var ErrFollowLimitExceeded = errors.New("follow limit exceeded")

type FollowRepo struct {
	db *gorm.DB
}

func NewFollowRepo(db *gorm.DB) *FollowRepo { return &FollowRepo{db: db} }

// Follow 在事务内幂等建立关注关系、维护双方计数，并在被关注方粉丝数达到阈值时原子晋升 is_bigv（只升不自动降）。
//
// 防死锁铁律：
//  1. 事务外按升序单行 autocommit 初始化缺失的 user_follow_stats 行，避免事务内 INSERT IGNORE 残留 S 锁再升级 X 锁引发死锁；
//  2. 事务内严格按 user_id 升序 FOR UPDATE 锁定双方 user_follow_stats（精准 Record X-Lock，无 Gap Lock）；
//  3. 再写入 user_follows 并更新双方计数，确保与双向互关、并发取关共享同一加锁全序。
func (r *FollowRepo) Follow(ctx context.Context, followerID, followeeID uint64, maxFollowing, bigVThreshold int, now time.Time) (model.FollowMutationResult, error) {
	if followerID == 0 || followeeID == 0 || followerID == followeeID {
		return model.FollowMutationResult{}, fmt.Errorf("invalid follow pair (%d, %d)", followerID, followeeID)
	}
	minID, maxID := orderedPair(followerID, followeeID)
	if err := r.ensureStatsRows(ctx, minID, maxID, now); err != nil {
		return model.FollowMutationResult{}, err
	}

	var result model.FollowMutationResult
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var statsRows []model.UserFollowStats
		if err := tx.Raw(
			"SELECT user_id, following_count, follower_count, is_bigv FROM user_follow_stats WHERE user_id IN (?, ?) ORDER BY user_id ASC FOR UPDATE",
			minID, maxID,
		).Scan(&statsRows).Error; err != nil {
			return fmt.Errorf("lock follow stats (%d, %d): %w", minID, maxID, err)
		}

		var followerStats, followeeStats *model.UserFollowStats
		for i := range statsRows {
			switch statsRows[i].UserID {
			case followerID:
				followerStats = &statsRows[i]
			case followeeID:
				followeeStats = &statsRows[i]
			}
		}
		if followerStats == nil {
			return fmt.Errorf("follow stats row missing for user %d", followerID)
		}
		if followeeStats == nil {
			return fmt.Errorf("follow stats row missing for user %d", followeeID)
		}

		wasBigV := followeeStats.IsBigV != 0
		result.FolloweeIsBigV = wasBigV

		if maxFollowing <= 0 || uint64(followerStats.FollowingCount) >= uint64(maxFollowing) {
			exists, err := existsInDB(tx, followerID, followeeID)
			if err != nil {
				return err
			}
			if exists {
				return nil
			}
			return ErrFollowLimitExceeded
		}

		res := tx.Exec(
			"INSERT IGNORE INTO user_follows (follower_id, followee_id, created_at) VALUES (?, ?, ?)",
			followerID, followeeID, now,
		)
		if res.Error != nil {
			return fmt.Errorf("insert follow (%d -> %d): %w", followerID, followeeID, res.Error)
		}
		if res.RowsAffected == 0 {
			return nil
		}

		if err := tx.Exec(
			"UPDATE user_follow_stats SET following_count = following_count + 1, updated_at = ? WHERE user_id = ?",
			now, followerID,
		).Error; err != nil {
			return fmt.Errorf("increment following_count for %d: %w", followerID, err)
		}

		if bigVThreshold > 0 {
			if err := tx.Exec(
				"UPDATE user_follow_stats SET follower_count = follower_count + 1, is_bigv = IF(follower_count + 1 >= ?, 1, is_bigv), updated_at = ? WHERE user_id = ?",
				bigVThreshold, now, followeeID,
			).Error; err != nil {
				return fmt.Errorf("increment follower_count for %d: %w", followeeID, err)
			}
		} else {
			if err := tx.Exec(
				"UPDATE user_follow_stats SET follower_count = follower_count + 1, updated_at = ? WHERE user_id = ?",
				now, followeeID,
			).Error; err != nil {
				return fmt.Errorf("increment follower_count for %d: %w", followeeID, err)
			}
		}

		nowBigV := wasBigV || (bigVThreshold > 0 && uint64(followeeStats.FollowerCount)+1 >= uint64(bigVThreshold))
		result = model.FollowMutationResult{
			Changed:        true,
			FolloweeIsBigV: nowBigV,
			PromotedBigV:   !wasBigV && nowBigV,
		}
		return nil
	})
	if err != nil {
		return model.FollowMutationResult{}, err
	}
	return result, nil
}

// Unfollow 在事务内幂等解除关注关系并维护双方计数（不回退 is_bigv，遵循只升不自动降原则）。
// 事务外先走唯一索引短路未关注请求；事务内严格遵循与 Follow 相同的升序 FOR UPDATE 加锁顺序。
func (r *FollowRepo) Unfollow(ctx context.Context, followerID, followeeID uint64, now time.Time) (model.FollowMutationResult, error) {
	if followerID == 0 || followeeID == 0 || followerID == followeeID {
		return model.FollowMutationResult{}, nil
	}

	exists, err := r.Exists(ctx, followerID, followeeID)
	if err != nil {
		return model.FollowMutationResult{}, err
	}
	if !exists {
		return model.FollowMutationResult{}, nil
	}

	minID, maxID := orderedPair(followerID, followeeID)
	var result model.FollowMutationResult
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var statsRows []model.UserFollowStats
		if err := tx.Raw(
			"SELECT user_id, following_count, follower_count, is_bigv FROM user_follow_stats WHERE user_id IN (?, ?) ORDER BY user_id ASC FOR UPDATE",
			minID, maxID,
		).Scan(&statsRows).Error; err != nil {
			return fmt.Errorf("lock follow stats (%d, %d): %w", minID, maxID, err)
		}

		var followeeIsBigV bool
		for i := range statsRows {
			if statsRows[i].UserID == followeeID {
				followeeIsBigV = statsRows[i].IsBigV != 0
				break
			}
		}
		result.FolloweeIsBigV = followeeIsBigV

		res := tx.Exec(
			"DELETE FROM user_follows WHERE follower_id = ? AND followee_id = ?",
			followerID, followeeID,
		)
		if res.Error != nil {
			return fmt.Errorf("delete follow (%d -> %d): %w", followerID, followeeID, res.Error)
		}
		if res.RowsAffected == 0 {
			return nil
		}

		// 使用 CASE 防止 MySQL 8 INT UNSIGNED 在 0-1 时触发 ERROR 1690 无符号下溢。
		if err := tx.Exec(
			"UPDATE user_follow_stats SET following_count = CASE WHEN following_count > 0 THEN following_count - 1 ELSE 0 END, updated_at = ? WHERE user_id = ?",
			now, followerID,
		).Error; err != nil {
			return fmt.Errorf("decrement following_count for %d: %w", followerID, err)
		}
		if err := tx.Exec(
			"UPDATE user_follow_stats SET follower_count = CASE WHEN follower_count > 0 THEN follower_count - 1 ELSE 0 END, updated_at = ? WHERE user_id = ?",
			now, followeeID,
		).Error; err != nil {
			return fmt.Errorf("decrement follower_count for %d: %w", followeeID, err)
		}

		result.Changed = true
		return nil
	})
	if err != nil {
		return model.FollowMutationResult{}, err
	}
	return result, nil
}

// Exists 检查 followerID 是否已关注 followeeID（命中 uk_follower_followee）。
func (r *FollowRepo) Exists(ctx context.Context, followerID, followeeID uint64) (bool, error) {
	if followerID == 0 || followeeID == 0 || followerID == followeeID {
		return false, nil
	}
	return existsInDB(r.db.WithContext(ctx), followerID, followeeID)
}

// BatchIsFollowing 批量检查 followerID 对一批目标用户的关注状态（命中 uk_follower_followee）。
func (r *FollowRepo) BatchIsFollowing(ctx context.Context, followerID uint64, followeeIDs []uint64) (map[uint64]bool, error) {
	out := make(map[uint64]bool, len(followeeIDs))
	if followerID == 0 || len(followeeIDs) == 0 {
		return out, nil
	}
	var matched []uint64
	if err := r.db.WithContext(ctx).
		Model(&model.UserFollow{}).
		Where("follower_id = ? AND followee_id IN ?", followerID, followeeIDs).
		Pluck("followee_id", &matched).Error; err != nil {
		return nil, fmt.Errorf("batch is following for %d: %w", followerID, err)
	}
	for _, id := range matched {
		out[id] = true
	}
	return out, nil
}

// GetStatsByUserIDs 批量读取用户关注计数与大 V 标识；无行的用户不放入返回 map，交由三级缓存写入空值标记。
func (r *FollowRepo) GetStatsByUserIDs(ctx context.Context, userIDs []uint64) (map[uint64]model.FollowStats, error) {
	out := make(map[uint64]model.FollowStats, len(userIDs))
	if len(userIDs) == 0 {
		return out, nil
	}
	var rows []model.UserFollowStats
	if err := r.db.WithContext(ctx).
		Select("user_id", "following_count", "follower_count", "is_bigv").
		Where("user_id IN ?", userIDs).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("get follow stats by user ids: %w", err)
	}
	for _, row := range rows {
		out[row.UserID] = model.FollowStats{
			FollowingCount: int64(row.FollowingCount),
			FollowerCount:  int64(row.FollowerCount),
			IsBigV:         row.IsBigV != 0,
		}
	}
	return out, nil
}

// GetStats 直接从 MySQL 读取单用户关注计数与大 V 标识（无行时返回零值；供模块 04 Worker 实时判定扇出策略）。
func (r *FollowRepo) GetStats(ctx context.Context, userID uint64) (model.FollowStats, error) {
	if userID == 0 {
		return model.FollowStats{}, nil
	}
	m, err := r.GetStatsByUserIDs(ctx, []uint64{userID})
	if err != nil {
		return model.FollowStats{}, err
	}
	return m[userID], nil
}

// ListBigVIDs 拉取全站所有大 V（is_bigv = 1）用户 ID（命中 idx_bigv 覆盖索引）。
func (r *FollowRepo) ListBigVIDs(ctx context.Context) ([]uint64, error) {
	var ids []uint64
	if err := r.db.WithContext(ctx).
		Model(&model.UserFollowStats{}).
		Where("is_bigv = 1").
		Pluck("user_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("list bigv user ids: %w", err)
	}
	return ids, nil
}

// ListFollowingBefore 按关系主键 id 倒序扫描目标用户的关注列表（命中 idx_follower_list 覆盖索引，零回表）。
func (r *FollowRepo) ListFollowingBefore(ctx context.Context, followerID, beforeID uint64, limit int) ([]model.FollowEdge, error) {
	if limit <= 0 {
		return nil, nil
	}
	q := r.db.WithContext(ctx).
		Model(&model.UserFollow{}).
		Select("id, followee_id AS user_id").
		Where("follower_id = ?", followerID)
	if beforeID > 0 {
		q = q.Where("id < ?", beforeID)
	}
	var edges []model.FollowEdge
	if err := q.Order("id DESC").Limit(limit).Scan(&edges).Error; err != nil {
		return nil, fmt.Errorf("list following for %d before %d: %w", followerID, beforeID, err)
	}
	return edges, nil
}

// ListFollowersBefore 按关系主键 id 倒序扫描目标用户的粉丝列表（命中 idx_followee_list 覆盖索引，零回表）。
func (r *FollowRepo) ListFollowersBefore(ctx context.Context, followeeID, beforeID uint64, limit int) ([]model.FollowEdge, error) {
	if limit <= 0 {
		return nil, nil
	}
	q := r.db.WithContext(ctx).
		Model(&model.UserFollow{}).
		Select("id, follower_id AS user_id").
		Where("followee_id = ?", followeeID)
	if beforeID > 0 {
		q = q.Where("id < ?", beforeID)
	}
	var edges []model.FollowEdge
	if err := q.Order("id DESC").Limit(limit).Scan(&edges).Error; err != nil {
		return nil, fmt.Errorf("list followers for %d before %d: %w", followeeID, beforeID, err)
	}
	return edges, nil
}

// ListFollowerIDsBefore 为模块 04 预留：按关系主键 id 倒序分批拉取粉丝 ID，同时返回本批末条关系 id 作为下批 cursorID。
func (r *FollowRepo) ListFollowerIDsBefore(ctx context.Context, followeeID, cursorID uint64, limit int) ([]uint64, uint64, error) {
	edges, err := r.ListFollowersBefore(ctx, followeeID, cursorID, limit)
	if err != nil {
		return nil, 0, err
	}
	ids := make([]uint64, len(edges))
	for i, e := range edges {
		ids[i] = e.UserID
	}
	var nextCursorID uint64
	if len(edges) > 0 {
		nextCursorID = edges[len(edges)-1].ID
	}
	return ids, nextCursorID, nil
}

// ListAllFolloweeIDs 为模块 04 预留：拉取用户全部关注对象 ID（上限受 follow.max_following 约束，命中覆盖索引）。
func (r *FollowRepo) ListAllFolloweeIDs(ctx context.Context, followerID uint64) ([]uint64, error) {
	var ids []uint64
	if err := r.db.WithContext(ctx).
		Model(&model.UserFollow{}).
		Where("follower_id = ?", followerID).
		Pluck("followee_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("list all followee ids for %d: %w", followerID, err)
	}
	return ids, nil
}

// ensureStatsRows 在事务外按升序确保双方统计行存在，避免事务内 INSERT IGNORE 遗留 S 锁与后续 FOR UPDATE 产生锁升级死锁。
func (r *FollowRepo) ensureStatsRows(ctx context.Context, minID, maxID uint64, now time.Time) error {
	var existing []uint64
	if err := r.db.WithContext(ctx).
		Model(&model.UserFollowStats{}).
		Where("user_id IN ?", []uint64{minID, maxID}).
		Pluck("user_id", &existing).Error; err != nil {
		return fmt.Errorf("check follow stats rows (%d, %d): %w", minID, maxID, err)
	}
	hasMin, hasMax := false, false
	for _, id := range existing {
		switch id {
		case minID:
			hasMin = true
		case maxID:
			hasMax = true
		}
	}
	for _, id := range []uint64{minID, maxID} {
		if (id == minID && hasMin) || (id == maxID && hasMax) {
			continue
		}
		if err := r.db.WithContext(ctx).Exec(
			"INSERT IGNORE INTO user_follow_stats (user_id, following_count, follower_count, updated_at) VALUES (?, 0, 0, ?)",
			id, now,
		).Error; err != nil {
			return fmt.Errorf("init follow stats row %d: %w", id, err)
		}
	}
	return nil
}

func existsInDB(db *gorm.DB, followerID, followeeID uint64) (bool, error) {
	var one int
	err := db.Model(&model.UserFollow{}).
		Select("1").
		Where("follower_id = ? AND followee_id = ?", followerID, followeeID).
		Limit(1).
		Take(&one).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check follow exists (%d -> %d): %w", followerID, followeeID, err)
	}
	return true, nil
}

func orderedPair(a, b uint64) (uint64, uint64) {
	if a < b {
		return a, b
	}
	return b, a
}
