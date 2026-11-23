package repository

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"wallpaper-feed/internal/model"
)

var (
	// ErrFollowLimitReached 表示关注操作超出系统允许的最大关注数上限。
	ErrFollowLimitReached = errors.New("repository: follow limit reached")

	errFollowStatsRowsMissing = errors.New("repository: follow stats rows missing")
)

// FollowTxResult 描述一次关注或取关事务提交后的最新结果。
type FollowTxResult struct {
	Changed       bool
	FollowerCount int64
	IsBigV        bool
}

// FollowRepository 封装 user_follows 与 user_follow_stats 的防死锁写事务与覆盖索引读查询。
type FollowRepository struct {
	db *gorm.DB
}

// NewFollowRepository 构造关注关系仓储。
func NewFollowRepository(db *gorm.DB) *FollowRepository {
	return &FollowRepository{db: db}
}

// Follow 执行幂等关注写事务：按 user_id ASC 全序锁双行防死锁，冷启动缺行时在事务外预建行后重入，
// 幂等校验优先于上限校验，目标粉丝达到 bigVThreshold 时原子单向晋升 is_bigv=1。
func (r *FollowRepository) Follow(
	ctx context.Context,
	followerID, followeeID uint64,
	maxFollowing, bigVThreshold int64,
) (FollowTxResult, error) {
	if followerID == 0 || followeeID == 0 || followerID == followeeID {
		return FollowTxResult{}, errors.New("repository: invalid follow pair")
	}
	minID, maxID := min(followerID, followeeID), max(followerID, followeeID)

	res, err := r.followOnce(ctx, followerID, followeeID, minID, maxID, maxFollowing, bigVThreshold)
	if errors.Is(err, errFollowStatsRowsMissing) {
		if ensureErr := r.ensureStatsRows(ctx, minID, maxID); ensureErr != nil {
			return FollowTxResult{}, ensureErr
		}
		return r.followOnce(ctx, followerID, followeeID, minID, maxID, maxFollowing, bigVThreshold)
	}
	return res, err
}

func (r *FollowRepository) followOnce(
	ctx context.Context,
	followerID, followeeID, minID, maxID uint64,
	maxFollowing, bigVThreshold int64,
) (FollowTxResult, error) {
	var res FollowTxResult
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		followerStats, followeeStats, err := lockStatsPair(tx, followerID, followeeID, minID, maxID)
		if err != nil {
			return err
		}

		// 事务首句已对双方加 X 锁，此处为锁后首次普通快照读，确保读取最新已提交关系且不加 Gap Lock。
		var existingID uint64
		err = tx.Table(model.UserFollow{}.TableName()).
			Select("id").
			Where("follower_id = ? AND followee_id = ?", followerID, followeeID).
			Limit(1).
			Take(&existingID).Error
		if err == nil {
			res = FollowTxResult{
				Changed:       false,
				FollowerCount: int64(followeeStats.FollowerCount),
				IsBigV:        followeeStats.IsBigV == 1,
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check follow exists: %w", err)
		}

		if maxFollowing > 0 && int64(followerStats.FollowingCount) >= maxFollowing {
			return ErrFollowLimitReached
		}

		edge := model.UserFollow{
			FollowerID: followerID,
			FolloweeID: followeeID,
		}
		if err := tx.Create(&edge).Error; err != nil {
			if isDuplicateKeyError(err) {
				res = FollowTxResult{
					Changed:       false,
					FollowerCount: int64(followeeStats.FollowerCount),
					IsBigV:        followeeStats.IsBigV == 1,
				}
				return nil
			}
			return fmt.Errorf("insert user follow: %w", err)
		}

		newFollowing := followerStats.FollowingCount + 1
		newFollower := followeeStats.FollowerCount + 1
		newIsBigV := followeeStats.IsBigV
		if newIsBigV == 0 && bigVThreshold > 0 && int64(newFollower) >= bigVThreshold {
			newIsBigV = 1
		}

		if err := updateStatsPairOrdered(tx, followerID, newFollowing, followeeID, newFollower, newIsBigV); err != nil {
			return err
		}

		res = FollowTxResult{
			Changed:       true,
			FollowerCount: int64(newFollower),
			IsBigV:        newIsBigV == 1,
		}
		return nil
	})
	return res, err
}

// Unfollow 执行幂等取关写事务：按 user_id ASC 全序锁双行，按主键精准删除零二级索引 Gap Lock，
// 保留大 V 标记不自动降级；对从未关注过的新用户不写入垃圾空统计行。
func (r *FollowRepository) Unfollow(ctx context.Context, followerID, followeeID uint64) (FollowTxResult, error) {
	if followerID == 0 || followeeID == 0 || followerID == followeeID {
		return FollowTxResult{}, errors.New("repository: invalid unfollow pair")
	}
	minID, maxID := min(followerID, followeeID), max(followerID, followeeID)

	res, err := r.unfollowOnce(ctx, followerID, followeeID, minID, maxID)
	if !errors.Is(err, errFollowStatsRowsMissing) {
		return res, err
	}

	// 统计行不全时先在事务外确认是否真存在关注记录，防止恶意 DELETE 为未互动用户刷出空统计行。
	var existingID uint64
	checkErr := r.db.WithContext(ctx).
		Table(model.UserFollow{}.TableName()).
		Select("id").
		Where("follower_id = ? AND followee_id = ?", followerID, followeeID).
		Limit(1).
		Take(&existingID).Error
	if errors.Is(checkErr, gorm.ErrRecordNotFound) {
		stats, statsErr := r.FindStatsByUserID(ctx, followeeID)
		if statsErr != nil {
			return FollowTxResult{}, statsErr
		}
		return FollowTxResult{
			Changed:       false,
			FollowerCount: stats.FollowerCount,
			IsBigV:        stats.IsBigV,
		}, nil
	}
	if checkErr != nil {
		return FollowTxResult{}, fmt.Errorf("check follow exists before unfollow ensure: %w", checkErr)
	}

	if ensureErr := r.ensureStatsRows(ctx, minID, maxID); ensureErr != nil {
		return FollowTxResult{}, ensureErr
	}
	return r.unfollowOnce(ctx, followerID, followeeID, minID, maxID)
}

func (r *FollowRepository) unfollowOnce(
	ctx context.Context,
	followerID, followeeID, minID, maxID uint64,
) (FollowTxResult, error) {
	var res FollowTxResult
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		followerStats, followeeStats, err := lockStatsPair(tx, followerID, followeeID, minID, maxID)
		if err != nil {
			return err
		}

		var existingID uint64
		err = tx.Table(model.UserFollow{}.TableName()).
			Select("id").
			Where("follower_id = ? AND followee_id = ?", followerID, followeeID).
			Limit(1).
			Take(&existingID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			res = FollowTxResult{
				Changed:       false,
				FollowerCount: int64(followeeStats.FollowerCount),
				IsBigV:        followeeStats.IsBigV == 1,
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("find follow edge for delete: %w", err)
		}

		del := tx.Where("id = ?", existingID).Delete(&model.UserFollow{})
		if del.Error != nil {
			return fmt.Errorf("delete user follow by id: %w", del.Error)
		}
		if del.RowsAffected == 0 {
			res = FollowTxResult{
				Changed:       false,
				FollowerCount: int64(followeeStats.FollowerCount),
				IsBigV:        followeeStats.IsBigV == 1,
			}
			return nil
		}

		newFollowing := followerStats.FollowingCount
		if newFollowing > 0 {
			newFollowing--
		}
		newFollower := followeeStats.FollowerCount
		if newFollower > 0 {
			newFollower--
		}

		if err := updateStatsPairOrdered(tx, followerID, newFollowing, followeeID, newFollower, followeeStats.IsBigV); err != nil {
			return err
		}

		res = FollowTxResult{
			Changed:       true,
			FollowerCount: int64(newFollower),
			IsBigV:        followeeStats.IsBigV == 1,
		}
		return nil
	})
	return res, err
}

// ensureStatsRows 在事务外独立 autocommit 语句中按 user_id ASC 幂等预建统计行，
// 微秒级提交即释锁，彻底规避事务内 Gap Lock 与 INSERT IGNORE S->X 锁升级死锁。
func (r *FollowRepository) ensureStatsRows(ctx context.Context, minID, maxID uint64) error {
	if err := r.db.WithContext(ctx).Exec(
		"INSERT IGNORE INTO "+model.UserFollowStats{}.TableName()+" (user_id) VALUES (?), (?)",
		minID, maxID,
	).Error; err != nil {
		return fmt.Errorf("ensure follow stats rows: %w", err)
	}
	return nil
}

// FindStatsByUserID 查询单个用户的关注统计；懒初始化表查无行时返回全零统计而非错误。
func (r *FollowRepository) FindStatsByUserID(ctx context.Context, userID uint64) (model.FollowStatsCache, error) {
	var row model.UserFollowStats
	err := r.db.WithContext(ctx).
		Select("user_id, following_count, follower_count, is_bigv").
		Where("user_id = ?", userID).
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.FollowStatsCache{}, nil
	}
	if err != nil {
		return model.FollowStatsCache{}, fmt.Errorf("find follow stats by user_id: %w", err)
	}
	return row.ToStatsCache(), nil
}

// FindStatsByUserIDs 批量查询多用户关注统计；未初始化行自动补零值返回。
func (r *FollowRepository) FindStatsByUserIDs(ctx context.Context, userIDs []uint64) (map[uint64]model.FollowStatsCache, error) {
	out := make(map[uint64]model.FollowStatsCache, len(userIDs))
	if len(userIDs) == 0 {
		return out, nil
	}
	for _, uid := range userIDs {
		out[uid] = model.FollowStatsCache{}
	}

	var rows []model.UserFollowStats
	if err := r.db.WithContext(ctx).
		Select("user_id, following_count, follower_count, is_bigv").
		Where("user_id IN ?", userIDs).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("find follow stats by user_ids: %w", err)
	}
	for _, row := range rows {
		out[row.UserID] = row.ToStatsCache()
	}
	return out, nil
}

// ListAllFollowingIDs 经 uk_follower_followee 覆盖索引顺序扫描用户全部关注对象 ID（严格升序）。
func (r *FollowRepository) ListAllFollowingIDs(ctx context.Context, followerID uint64) ([]uint64, error) {
	ids := make([]uint64, 0)
	if err := r.db.WithContext(ctx).
		Table(model.UserFollow{}.TableName()).
		Select("followee_id").
		Where("follower_id = ?", followerID).
		Order("followee_id ASC").
		Pluck("followee_id", &ids).Error; err != nil {
		return nil, fmt.Errorf("list all following ids: %w", err)
	}
	return ids, nil
}

// ListFollowingPage 经 idx_follower_list 覆盖索引零回表倒序查询用户关注列表页。
func (r *FollowRepository) ListFollowingPage(ctx context.Context, followerID, beforeID uint64, limit int) ([]model.FollowEdge, error) {
	return r.listFollowEdges(ctx, "follower_id", "followee_id", followerID, beforeID, limit)
}

// ListFollowerPage 经 idx_followee_list 覆盖索引零回表倒序查询用户粉丝列表页。
func (r *FollowRepository) ListFollowerPage(ctx context.Context, followeeID, beforeID uint64, limit int) ([]model.FollowEdge, error) {
	return r.listFollowEdges(ctx, "followee_id", "follower_id", followeeID, beforeID, limit)
}

func (r *FollowRepository) listFollowEdges(
	ctx context.Context,
	filterCol, peerCol string,
	userID, beforeID uint64,
	limit int,
) ([]model.FollowEdge, error) {
	if limit <= 0 {
		return []model.FollowEdge{}, nil
	}

	q := r.db.WithContext(ctx).
		Table(model.UserFollow{}.TableName()).
		Select("id, "+peerCol+" AS peer_id").
		Where(filterCol+" = ?", userID)
	if beforeID > 0 {
		q = q.Where("id < ?", beforeID)
	}

	var edges []model.FollowEdge
	if err := q.Order("id DESC").Limit(limit).Scan(&edges).Error; err != nil {
		return nil, fmt.Errorf("list follow edges by %s: %w", filterCol, err)
	}
	return edges, nil
}

func lockStatsPair(
	tx *gorm.DB,
	followerID, followeeID, minID, maxID uint64,
) (followerStats, followeeStats model.UserFollowStats, err error) {
	var rows []model.UserFollowStats
	if err := tx.Select("user_id, following_count, follower_count, is_bigv").
		Where("user_id IN ?", []uint64{minID, maxID}).
		Order("user_id ASC").
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Find(&rows).Error; err != nil {
		return followerStats, followeeStats, fmt.Errorf("lock follow stats pair: %w", err)
	}
	if len(rows) < 2 {
		return followerStats, followeeStats, errFollowStatsRowsMissing
	}

	for _, row := range rows {
		switch row.UserID {
		case followerID:
			followerStats = row
		case followeeID:
			followeeStats = row
		}
	}
	return followerStats, followeeStats, nil
}

func updateStatsPairOrdered(
	tx *gorm.DB,
	followerID, newFollowing uint64,
	followeeID, newFollower uint64,
	newIsBigV uint8,
) error {
	updateFollower := func() error {
		if err := tx.Model(&model.UserFollowStats{}).
			Where("user_id = ?", followerID).
			Updates(map[string]any{"following_count": newFollowing}).Error; err != nil {
			return fmt.Errorf("update follower following_count: %w", err)
		}
		return nil
	}
	updateFollowee := func() error {
		if err := tx.Model(&model.UserFollowStats{}).
			Where("user_id = ?", followeeID).
			Updates(map[string]any{
				"follower_count": newFollower,
				"is_bigv":        newIsBigV,
			}).Error; err != nil {
			return fmt.Errorf("update followee follower_count: %w", err)
		}
		return nil
	}

	if followerID < followeeID {
		if err := updateFollower(); err != nil {
			return err
		}
		return updateFollowee()
	}
	if err := updateFollowee(); err != nil {
		return err
	}
	return updateFollower()
}
