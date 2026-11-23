package repository

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"wallpaper-feed/internal/model"
)

func TestNormalizeStatsDeltasAndBuildBatchSQL(t *testing.T) {
	merged := normalizeStatsDeltas([]model.StatsDelta{
		{WallpaperID: 30, Like: 1},
		{WallpaperID: 10, Like: 2, Download: 3},
		{WallpaperID: 20, Fav: 1},
		{WallpaperID: 20, Fav: -1}, // 互相抵消应被剔除
		{WallpaperID: 10, Like: -1, Comment: 2},
		{WallpaperID: 0, Like: 5}, // 非法 ID 忽略
	})

	if len(merged) != 2 {
		t.Fatalf("expected 2 non-zero sorted deltas, got %+v", merged)
	}
	if merged[0] != (model.StatsDelta{WallpaperID: 10, Like: 1, Download: 3, Comment: 2}) {
		t.Fatalf("merged[0] mismatch: %+v", merged[0])
	}
	if merged[1] != (model.StatsDelta{WallpaperID: 30, Like: 1}) {
		t.Fatalf("merged[1] mismatch: %+v", merged[1])
	}

	sqlStr, args := buildBatchStatsUpsertSQL(merged)
	if len(args) != 10 {
		t.Fatalf("expected 10 bind args for 2 rows, got %d", len(args))
	}
	for _, snippet := range []string{
		"INSERT INTO wallpaper_stats",
		"CAST(? AS UNSIGNED) AS wallpaper_id",
		"GREATEST(d.dl, 0)",
		"CAST(-d.dl AS UNSIGNED)",
		"ON DUPLICATE KEY UPDATE",
		"UNION ALL",
	} {
		if !strings.Contains(sqlStr, snippet) {
			t.Fatalf("batch sql missing %q:\n%s", snippet, sqlStr)
		}
	}
}

func openTestInteractionRepo(t *testing.T) (*InteractionRepository, *gorm.DB) {
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

	return NewInteractionRepository(db), db
}

func cleanupTestInteractions(t *testing.T, db *gorm.DB, wpIDs []uint64) {
	t.Helper()
	cleanup := func() {
		_ = db.Where("wallpaper_id IN ?", wpIDs).Delete(&model.WallpaperStats{}).Error
		_ = db.Where("wallpaper_id IN ?", wpIDs).Delete(&model.WallpaperLike{}).Error
		_ = db.Where("wallpaper_id IN ?", wpIDs).Delete(&model.WallpaperFavorite{}).Error
		_ = db.Where("wallpaper_id IN ?", wpIDs).Delete(&model.Comment{}).Error
	}
	cleanup()
	t.Cleanup(cleanup)
}

func TestInteractionRepository_ConcurrentIdempotentAddRemove(t *testing.T) {
	repo, db := openTestInteractionRepo(t)
	ctx := context.Background()
	const (
		userID uint64 = 910000001
		wpID   uint64 = 910000101
	)
	cleanupTestInteractions(t, db, []uint64{wpID})

	var (
		wg          sync.WaitGroup
		addChanged  atomic.Int64
		addErrCount atomic.Int64
	)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			changed, err := repo.AddRelation(ctx, model.InteractionKindLike, userID, wpID)
			if err != nil {
				addErrCount.Add(1)
				return
			}
			if changed {
				addChanged.Add(1)
			}
		}()
	}
	wg.Wait()
	if addErrCount.Load() != 0 || addChanged.Load() != 1 {
		t.Fatalf("concurrent add expected changed=1 err=0, got changed=%d err=%d", addChanged.Load(), addErrCount.Load())
	}

	var (
		remChanged  atomic.Int64
		remErrCount atomic.Int64
	)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			changed, err := repo.RemoveRelation(ctx, model.InteractionKindLike, userID, wpID)
			if err != nil {
				remErrCount.Add(1)
				return
			}
			if changed {
				remChanged.Add(1)
			}
		}()
	}
	wg.Wait()
	if remErrCount.Load() != 0 || remChanged.Load() != 1 {
		t.Fatalf("concurrent remove expected changed=1 err=0, got changed=%d err=%d", remChanged.Load(), remErrCount.Load())
	}
}

func TestInteractionRepository_BatchApplyStatsDeltas_NoUnderflowAndZeroDeadlock(t *testing.T) {
	repo, db := openTestInteractionRepo(t)
	ctx := context.Background()
	wpIDs := []uint64{910000201, 910000202, 910000203}
	cleanupTestInteractions(t, db, wpIDs)

	// 1. 冷图直接负增量不得触发 MySQL 1264 或 1690 错误，应安全落为 0。
	if err := repo.BatchApplyStatsDeltas(ctx, []model.StatsDelta{
		{WallpaperID: wpIDs[0], Like: -3, Fav: -1, Comment: -2},
	}); err != nil {
		t.Fatalf("cold negative delta failed: %v", err)
	}
	st0, err := repo.FindStatsByWallpaperID(ctx, wpIDs[0])
	if err != nil || st0 != (model.WallpaperStatsCache{}) {
		t.Fatalf("expected zero stats, got (%+v, %v)", st0, err)
	}

	// 2. 已有计数 2 扣减 -5 安全归 0（防 1690），并发逆序多壁纸刷盘 0 死锁（防 1213）。
	if err := repo.BatchApplyStatsDeltas(ctx, []model.StatsDelta{
		{WallpaperID: wpIDs[0], Like: 2, Download: 10},
	}); err != nil {
		t.Fatalf("seed delta failed: %v", err)
	}
	if err := repo.BatchApplyStatsDeltas(ctx, []model.StatsDelta{
		{WallpaperID: wpIDs[0], Like: -5, Download: 5},
	}); err != nil {
		t.Fatalf("underflow clamp delta failed: %v", err)
	}
	st0, err = repo.FindStatsByWallpaperID(ctx, wpIDs[0])
	if err != nil || st0.LikeCount != 0 || st0.DownloadCount != 15 {
		t.Fatalf("expected like=0 download=15, got (%+v, %v)", st0, err)
	}

	var (
		wg       sync.WaitGroup
		errCount atomic.Int64
	)
	for i := range 12 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			batch := []model.StatsDelta{
				{WallpaperID: wpIDs[2], Like: 1, Download: 1},
				{WallpaperID: wpIDs[0], Like: 1, Download: 1},
				{WallpaperID: wpIDs[1], Like: 1, Download: 1},
			}
			if idx%2 == 0 {
				batch[0], batch[2] = batch[2], batch[0]
			}
			for range 10 {
				if err := repo.BatchApplyStatsDeltas(ctx, batch); err != nil {
					errCount.Add(1)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if errCount.Load() != 0 {
		t.Fatalf("concurrent batch upsert encountered %d errors", errCount.Load())
	}

	// 3. 验证 CAS 乐观锁不覆盖并发热写且保留 download_count。
	rows, err := repo.FindStatsReconcileRows(ctx, []uint64{wpIDs[0]})
	if err != nil {
		t.Fatalf("FindStatsReconcileRows failed: %v", err)
	}
	staleTS := rows[wpIDs[0]].UpdatedAt.Add(-time.Second)
	updated, err := repo.CompareAndUpdateStats(ctx, wpIDs[0], 1, 0, 0, staleTS)
	if err != nil || updated {
		t.Fatalf("stale CAS must not update, got updated=%v err=%v", updated, err)
	}
	updated, err = repo.CompareAndUpdateStats(ctx, wpIDs[0], 7, 3, 2, rows[wpIDs[0]].UpdatedAt)
	if err != nil || !updated {
		t.Fatalf("matching CAS must update, got updated=%v err=%v", updated, err)
	}
	st0, err = repo.FindStatsByWallpaperID(ctx, wpIDs[0])
	if err != nil || st0.LikeCount != 7 || st0.FavCount != 3 || st0.CommentCount != 2 || st0.DownloadCount != 135 {
		t.Fatalf("CAS must keep download_count=135 and update 3 fields, got (%+v, %v)", st0, err)
	}
}
