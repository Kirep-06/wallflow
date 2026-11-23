package repository

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"wallpaper-feed/internal/model"
)

func openTestFollowRepo(t *testing.T) (*FollowRepository, *gorm.DB) {
	t.Helper()
	dsn := os.Getenv("WF_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("skip mysql integration test: WF_TEST_MYSQL_DSN is not set")
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger:         logger.Default.LogMode(logger.Silent),
		TranslateError: true,
	})
	if err != nil {
		t.Fatalf("open test mysql failed: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get sql.DB failed: %v", err)
	}
	sqlDB.SetMaxOpenConns(32)
	sqlDB.SetMaxIdleConns(16)
	t.Cleanup(func() { _ = sqlDB.Close() })

	return NewFollowRepository(db), db
}

func cleanupTestFollowUsers(t *testing.T, db *gorm.DB, userIDs []uint64) {
	t.Helper()
	cleanup := func() {
		_ = db.Where("follower_id IN ? OR followee_id IN ?", userIDs, userIDs).Delete(&model.UserFollow{}).Error
		_ = db.Where("user_id IN ?", userIDs).Delete(&model.UserFollowStats{}).Error
	}
	cleanup()
	t.Cleanup(cleanup)
}

func TestFollowRepository_ConcurrentMutualFollow_ZeroDeadlock(t *testing.T) {
	repo, db := openTestFollowRepo(t)
	ctx := context.Background()

	const (
		userA uint64 = 900000001
		userB uint64 = 900000002
	)
	cleanupTestFollowUsers(t, db, []uint64{userA, userB})

	const (
		workers = 16
		rounds  = 15
	)
	var wg sync.WaitGroup
	var errCount atomic.Int64
	errCh := make(chan error, workers*rounds)

	for i := range workers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for r := range rounds {
				var err error
				switch (idx + r) % 4 {
				case 0:
					_, err = repo.Follow(ctx, userA, userB, 2000, 1000)
				case 1:
					_, err = repo.Follow(ctx, userB, userA, 2000, 1000)
				case 2:
					_, err = repo.Unfollow(ctx, userA, userB)
				case 3:
					_, err = repo.Unfollow(ctx, userB, userA)
				}
				if err != nil {
					errCount.Add(1)
					select {
					case errCh <- err:
					default:
					}
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	if errCount.Load() > 0 {
		t.Fatalf("concurrent mutual follow encountered %d errors, first: %v", errCount.Load(), <-errCh)
	}

	// 校验统计表与关系明细表 100% 严格一致。
	for _, uid := range []uint64{userA, userB} {
		stats, err := repo.FindStatsByUserID(ctx, uid)
		if err != nil {
			t.Fatalf("FindStatsByUserID(%d) failed: %v", uid, err)
		}
		var realFollowing, realFollower int64
		_ = db.Model(&model.UserFollow{}).Where("follower_id = ?", uid).Count(&realFollowing).Error
		_ = db.Model(&model.UserFollow{}).Where("followee_id = ?", uid).Count(&realFollower).Error
		if stats.FollowingCount != realFollowing || stats.FollowerCount != realFollower {
			t.Fatalf("uid=%d stats=(%d,%d) != real=(%d,%d)", uid, stats.FollowingCount, stats.FollowerCount, realFollowing, realFollower)
		}
	}
}

func TestFollowRepository_ConcurrentLimitAndIdempotentPriority(t *testing.T) {
	repo, db := openTestFollowRepo(t)
	ctx := context.Background()

	const (
		actor    uint64 = 900000100
		maxLimit int64  = 5
		targets         = 20
	)
	allIDs := make([]uint64, 0, targets+1)
	allIDs = append(allIDs, actor)
	for i := range targets {
		allIDs = append(allIDs, 900000101+uint64(i))
	}
	cleanupTestFollowUsers(t, db, allIDs)

	var (
		wg       sync.WaitGroup
		okCount  atomic.Int64
		limCount atomic.Int64
		otherErr atomic.Int64
	)
	for _, targetID := range allIDs[1:] {
		wg.Add(1)
		go func(tid uint64) {
			defer wg.Done()
			res, err := repo.Follow(ctx, actor, tid, maxLimit, 1000)
			if err == nil && res.Changed {
				okCount.Add(1)
				return
			}
			if errors.Is(err, ErrFollowLimitReached) {
				limCount.Add(1)
				return
			}
			otherErr.Add(1)
		}(targetID)
	}
	wg.Wait()

	if otherErr.Load() != 0 || okCount.Load() != maxLimit || limCount.Load() != int64(targets)-maxLimit {
		t.Fatalf("limit race result: ok=%d limit=%d otherErr=%d", okCount.Load(), limCount.Load(), otherErr.Load())
	}

	// 已满上限时，重放已关注目标必须幂等成功（Changed=false），不可误报 ErrFollowLimitReached。
	following, err := repo.ListAllFollowingIDs(ctx, actor)
	if err != nil || int64(len(following)) != maxLimit {
		t.Fatalf("ListAllFollowingIDs = (%v, %v)", following, err)
	}
	dupRes, err := repo.Follow(ctx, actor, following[0], maxLimit, 1000)
	if err != nil || dupRes.Changed {
		t.Fatalf("idempotent follow at limit = (%+v, %v), want Changed=false nil err", dupRes, err)
	}
}

func TestFollowRepository_BigVOneWayPromotionAndNoGarbageStatsOnUnfollow(t *testing.T) {
	repo, db := openTestFollowRepo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const (
		creator uint64 = 900000200
		fan1    uint64 = 900000201
		fan2    uint64 = 900000202
		fan3    uint64 = 900000203
		ghostA  uint64 = 900000204
		ghostB  uint64 = 900000205
	)
	cleanupTestFollowUsers(t, db, []uint64{creator, fan1, fan2, fan3, ghostA, ghostB})

	// 对从未关注过的新用户执行 Unfollow，不得在 user_follow_stats 生成垃圾空行。
	res, err := repo.Unfollow(ctx, ghostA, ghostB)
	if err != nil || res.Changed || res.FollowerCount != 0 {
		t.Fatalf("unfollow never-followed = (%+v, %v)", res, err)
	}
	var ghostRowCount int64
	_ = db.Model(&model.UserFollowStats{}).Where("user_id IN ?", []uint64{ghostA, ghostB}).Count(&ghostRowCount).Error
	if ghostRowCount != 0 {
		t.Fatalf("expected 0 garbage stats rows for unfollowed newcomers, got %d", ghostRowCount)
	}

	// 阈值设为 3：前 2 粉不晋升，第 3 粉原子晋升 is_bigv=true。
	for i, fan := range []uint64{fan1, fan2, fan3} {
		r, err := repo.Follow(ctx, fan, creator, 2000, 3)
		if err != nil || !r.Changed {
			t.Fatalf("fan %d follow failed: (%+v, %v)", fan, r, err)
		}
		wantBigV := i == 2
		if r.IsBigV != wantBigV {
			t.Fatalf("after %d fans, IsBigV=%v, want %v", i+1, r.IsBigV, wantBigV)
		}
	}

	// 全部取关掉粉回 0，is_bigv 仍保持 true 不自动降级。
	for _, fan := range []uint64{fan1, fan2, fan3} {
		r, err := repo.Unfollow(ctx, fan, creator)
		if err != nil || !r.Changed || !r.IsBigV {
			t.Fatalf("fan %d unfollow = (%+v, %v), want Changed=true IsBigV=true", fan, r, err)
		}
	}
	finalStats, err := repo.FindStatsByUserID(ctx, creator)
	if err != nil || finalStats.FollowerCount != 0 || !finalStats.IsBigV {
		t.Fatalf("final creator stats = (%+v, %v), want FollowerCount=0 IsBigV=true", finalStats, err)
	}
}
