package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

// SeedConfig 控制造数规模。
type SeedConfig struct {
	Fans            int // 普通粉丝用户数（需 >= BigVFollowers，确保大 V 真实粉丝 >= 1000）
	NormalAuthors   int // 普通创作者数
	BigVAuthors     int // 大 V 创作者数
	BigVFollowers   int // 每个大 V 的粉丝数（>= 1000）
	ReaderNormalCnt int // 测试用户关注的普通作者数
	ReaderBigVCnt   int // 测试用户关注的大 V 数
	Wallpapers      int // 总壁纸数（2万~5万）
	Tags            int // 标签数
}

func defaultSeedConfig() SeedConfig {
	return SeedConfig{
		Fans:            1050,
		NormalAuthors:   160,
		BigVAuthors:     10,
		BigVFollowers:   1020,
		ReaderNormalCnt: 142,
		ReaderBigVCnt:   8,
		Wallpapers:      30000,
		Tags:            30,
	}
}

// SeedMeta 保存压测运行期所需的真实库内关键 ID。
type SeedMeta struct {
	ReaderID       uint64
	FanUserIDs     []uint64
	NormalAuthors  []uint64
	BigVAuthors    []uint64
	FollowedIDs    []uint64
	WallpaperIDs   []uint64 // 按 ID 倒序或热点排序采样
	HotWallpaperID uint64
	ColdWallpaper  uint64
	TotalUsers     int64
	TotalWallpapers int64
	TotalFollows   int64
}

// ensureBenchDatabase 创建压测专用库并按顺序应用 migrations/*.sql（幂等）。
func ensureBenchDatabase(ctx context.Context, host string, port int, user, pass, dbName, migrationsDir string) error {
	rootDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=UTC&multiStatements=true",
		user, pass, host, port)
	rawDB, err := sql.Open("mysql", rootDSN)
	if err != nil {
		return fmt.Errorf("open mysql root: %w", err)
	}
	defer func() { _ = rawDB.Close() }()

	if _, err := rawDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci", dbName,
	)); err != nil {
		return fmt.Errorf("create database %s: %w", dbName, err)
	}

	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=UTC&multiStatements=true",
		user, pass, host, port, dbName)
	migDB, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return fmt.Errorf("open mysql %s: %w", dbName, err)
	}
	defer func() { _ = migDB.Close() }()

	// 检查表是否已存在，若已初始化则跳过重复 ALTER TABLE
	var tableCount int
	if err := migDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = 'wallpaper_images'",
		dbName,
	).Scan(&tableCount); err != nil {
		return fmt.Errorf("check schema initialized: %w", err)
	}
	if tableCount > 0 {
		return nil
	}

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations dir %s: %w", migrationsDir, err)
	}
	var sqlFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			sqlFiles = append(sqlFiles, e.Name())
		}
	}
	slices.Sort(sqlFiles)

	for _, name := range sqlFiles {
		data, err := os.ReadFile(filepath.Join(migrationsDir, name))
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := migDB.ExecContext(ctx, string(data)); err != nil {
			return fmt.Errorf("exec migration %s: %w", name, err)
		}
	}
	return nil
}

// cleanBenchData 幂等清空压测专用库的业务表（保留预置 categories）与 Redis 压测 DB。
func cleanBenchData(ctx context.Context, db *gorm.DB, rdb *redis.Client) error {
	tables := []string{
		"comments",
		"wallpaper_likes",
		"wallpaper_favorites",
		"wallpaper_stats",
		"wallpaper_tags",
		"wallpaper_images",
		"wallpapers",
		"outbox_events",
		"user_follows",
		"user_follow_stats",
		"tags",
		"users",
	}
	for _, tbl := range tables {
		if err := db.WithContext(ctx).Exec("TRUNCATE TABLE " + tbl).Error; err != nil {
			return fmt.Errorf("truncate %s: %w", tbl, err)
		}
	}
	if rdb != nil {
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			return fmt.Errorf("redis flushdb: %w", err)
		}
	}
	return nil
}

// seedBenchData 高速批量灌入可复现压测数据集。
func seedBenchData(ctx context.Context, db *gorm.DB, rdb *redis.Client, cfg SeedConfig) (*SeedMeta, error) {
	start := time.Now()
	fmt.Printf("[seed] Cleaning existing bench data...\n")
	if err := cleanBenchData(ctx, db, rdb); err != nil {
		return nil, err
	}

	// 读取已有分类 ID
	var categoryIDs []uint64
	if err := db.WithContext(ctx).Table("categories").Select("id").Order("id ASC").Pluck("id", &categoryIDs).Error; err != nil {
		return nil, fmt.Errorf("load categories: %w", err)
	}
	if len(categoryIDs) == 0 {
		return nil, fmt.Errorf("categories table is empty, ensure migrations were applied")
	}

	// 仅算一次 bcrypt（cost=4 足够本地测试账号）
	pwHashBytes, err := bcrypt.GenerateFromPassword([]byte("BenchPass123!"), bcrypt.MinCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}
	pwHash := string(pwHashBytes)
	now := time.Now().UTC()

	// 1. 灌入用户：fans + normal authors + bigV authors + reader
	totalUsers := cfg.Fans + cfg.NormalAuthors + cfg.BigVAuthors + 1
	fmt.Printf("[seed] Inserting %d users (fans=%d, normal_authors=%d, bigv=%d, reader=1)...\n",
		totalUsers, cfg.Fans, cfg.NormalAuthors, cfg.BigVAuthors)

	userRows := make([]userSeedRow, 0, totalUsers)
	for i := 1; i <= cfg.Fans; i++ {
		userRows = append(userRows, userSeedRow{
			username: fmt.Sprintf("bench_fan_%04d", i),
			nickname: fmt.Sprintf("压测粉丝%04d", i),
		})
	}
	for i := 1; i <= cfg.NormalAuthors; i++ {
		userRows = append(userRows, userSeedRow{
			username: fmt.Sprintf("bench_author_%04d", i),
			nickname: fmt.Sprintf("普通画师%04d", i),
		})
	}
	for i := 1; i <= cfg.BigVAuthors; i++ {
		userRows = append(userRows, userSeedRow{
			username: fmt.Sprintf("bench_bigv_%04d", i),
			nickname: fmt.Sprintf("头部大V%02d", i),
		})
	}
	userRows = append(userRows, userSeedRow{
		username: "bench_reader",
		nickname: "压测核心读者",
	})

	if err := batchInsertUsers(ctx, db, userRows, pwHash, now); err != nil {
		return nil, err
	}

	// 回查各类用户 ID（TRUNCATE 后自增从 1 连续分配，仍显式查表确保严谨）
	meta, err := loadUserGroups(ctx, db)
	if err != nil {
		return nil, err
	}

	// 2. 灌入标签
	fmt.Printf("[seed] Inserting %d tags...\n", cfg.Tags)
	if err := batchInsertTags(ctx, db, cfg.Tags, now); err != nil {
		return nil, err
	}
	var tagIDs []uint64
	if err := db.WithContext(ctx).Table("tags").Select("id").Order("id ASC").Pluck("id", &tagIDs).Error; err != nil {
		return nil, fmt.Errorf("load tags: %w", err)
	}

	// 3. 灌入关注关系与统计（每个大 V >= 1000 粉丝；reader 关注 142 普通作者 + 8 大 V）
	fmt.Printf("[seed] Inserting follow graph (%d BigVs x %d followers + reader follows %d)...\n",
		len(meta.BigVAuthors), cfg.BigVFollowers, cfg.ReaderNormalCnt+cfg.ReaderBigVCnt)
	if err := seedFollowGraph(ctx, db, meta, cfg, now); err != nil {
		return nil, err
	}

	// 4. 灌入壁纸、图片子表、标签关联、互动统计（按时间交错分布于最近 30 天）
	fmt.Printf("[seed] Inserting %d wallpapers + wallpaper_images + wallpaper_stats + wallpaper_tags...\n", cfg.Wallpapers)
	if err := seedWallpapers(ctx, db, meta, categoryIDs, tagIDs, cfg.Wallpapers, now); err != nil {
		return nil, err
	}

	fullMeta, err := loadSeedMeta(ctx, db)
	if err != nil {
		return nil, err
	}
	fmt.Printf("[seed] Done in %s! users=%d, wallpapers=%d, follows=%d, reader_id=%d, hot_wp_id=%d\n",
		time.Since(start).Round(time.Millisecond),
		fullMeta.TotalUsers, fullMeta.TotalWallpapers, fullMeta.TotalFollows,
		fullMeta.ReaderID, fullMeta.HotWallpaperID,
	)
	return fullMeta, nil
}

type userSeedRow struct {
	username string
	nickname string
}

func batchInsertUsers(ctx context.Context, db *gorm.DB, rows []userSeedRow, pwHash string, now time.Time) error {
	const batchSize = 500
	ts := now.Format("2006-01-02 15:04:05.000")
	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		chunk := rows[start:end]
		var sb strings.Builder
		sb.WriteString("INSERT INTO users (username, password_hash, nickname, avatar_key, bio, created_at, updated_at) VALUES ")
		args := make([]any, 0, len(chunk)*4)
		for i, r := range chunk {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("(?, ?, ?, '', 'bench seed user', ?, ?)")
			args = append(args, r.username, pwHash, r.nickname, ts, ts)
		}
		if err := db.WithContext(ctx).Exec(sb.String(), args...).Error; err != nil {
			return fmt.Errorf("batch insert users: %w", err)
		}
	}
	return nil
}

func batchInsertTags(ctx context.Context, db *gorm.DB, count int, now time.Time) error {
	ts := now.Format("2006-01-02 15:04:05.000")
	var sb strings.Builder
	sb.WriteString("INSERT INTO tags (name, use_count, icon_key, created_at) VALUES ")
	args := make([]any, 0, count*2)
	for i := 1; i <= count; i++ {
		if i > 1 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?, 0, '', ?)")
		args = append(args, fmt.Sprintf("bench_tag_%02d", i), ts)
	}
	if err := db.WithContext(ctx).Exec(sb.String(), args...).Error; err != nil {
		return fmt.Errorf("batch insert tags: %w", err)
	}
	return nil
}

func seedFollowGraph(ctx context.Context, db *gorm.DB, meta *SeedMeta, cfg SeedConfig, now time.Time) error {
	type followEdge struct {
		follower uint64
		followee uint64
	}
	edges := make([]followEdge, 0, len(meta.BigVAuthors)*cfg.BigVFollowers+cfg.ReaderNormalCnt+cfg.ReaderBigVCnt)
	followingCnt := make(map[uint64]uint64, len(meta.FanUserIDs)+1)
	followerCnt := make(map[uint64]uint64, len(meta.BigVAuthors)+len(meta.NormalAuthors))
	isBigV := make(map[uint64]uint8, len(meta.BigVAuthors))

	// 每个大 V 拥有 cfg.BigVFollowers 个粉丝（>= 1000）
	fanLimit := min(cfg.BigVFollowers, len(meta.FanUserIDs))
	for _, bigVID := range meta.BigVAuthors {
		isBigV[bigVID] = 1
		for i := range fanLimit {
			fanID := meta.FanUserIDs[i]
			edges = append(edges, followEdge{follower: fanID, followee: bigVID})
			followingCnt[fanID]++
			followerCnt[bigVID]++
		}
	}

	// 核心测试用户 bench_reader 关注 ReaderNormalCnt 个普通作者 + ReaderBigVCnt 个大 V
	normalLimit := min(cfg.ReaderNormalCnt, len(meta.NormalAuthors))
	for i := range normalLimit {
		authorID := meta.NormalAuthors[i]
		edges = append(edges, followEdge{follower: meta.ReaderID, followee: authorID})
		followingCnt[meta.ReaderID]++
		followerCnt[authorID]++
	}
	bigVLimit := min(cfg.ReaderBigVCnt, len(meta.BigVAuthors))
	for i := range bigVLimit {
		bigVID := meta.BigVAuthors[i]
		edges = append(edges, followEdge{follower: meta.ReaderID, followee: bigVID})
		followingCnt[meta.ReaderID]++
		followerCnt[bigVID]++
	}

	// 批量写 user_follows
	ts := now.Format("2006-01-02 15:04:05.000")
	const batchSize = 1000
	for start := 0; start < len(edges); start += batchSize {
		end := min(start+batchSize, len(edges))
		chunk := edges[start:end]
		var sb strings.Builder
		sb.WriteString("INSERT INTO user_follows (follower_id, followee_id, created_at) VALUES ")
		args := make([]any, 0, len(chunk)*3)
		for i, e := range chunk {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("(?, ?, ?)")
			args = append(args, e.follower, e.followee, ts)
		}
		if err := db.WithContext(ctx).Exec(sb.String(), args...).Error; err != nil {
			return fmt.Errorf("batch insert user_follows: %w", err)
		}
	}

	// 汇总并批量写 user_follow_stats
	userSet := make(map[uint64]struct{}, len(followingCnt)+len(followerCnt))
	for uid := range followingCnt {
		userSet[uid] = struct{}{}
	}
	for uid := range followerCnt {
		userSet[uid] = struct{}{}
	}
	uids := make([]uint64, 0, len(userSet))
	for uid := range userSet {
		uids = append(uids, uid)
	}
	slices.Sort(uids)

	for start := 0; start < len(uids); start += batchSize {
		end := min(start+batchSize, len(uids))
		chunk := uids[start:end]
		var sb strings.Builder
		sb.WriteString("INSERT INTO user_follow_stats (user_id, following_count, follower_count, is_bigv, updated_at) VALUES ")
		args := make([]any, 0, len(chunk)*5)
		for i, uid := range chunk {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("(?, ?, ?, ?, ?)")
			args = append(args, uid, followingCnt[uid], followerCnt[uid], isBigV[uid], ts)
		}
		if err := db.WithContext(ctx).Exec(sb.String(), args...).Error; err != nil {
			return fmt.Errorf("batch insert user_follow_stats: %w", err)
		}
	}
	return nil
}

func seedWallpapers(
	ctx context.Context,
	db *gorm.DB,
	meta *SeedMeta,
	categoryIDs, tagIDs []uint64,
	total int,
	now time.Time,
) error {
	// 按时间从 30 天前均匀推进到当前，交替分配给普通作者（80%）与大 V（20%），
	// 使关注流中普通作者收件箱与大 V 发件箱作品真实交错。
	const batchSize = 500
	window := 29 * 24 * time.Hour // 全量落在 30 天内以覆盖热门榜扫描窗口
	step := window / time.Duration(max(total, 1))
	startTime := now.Add(-window)

	for batchStart := 0; batchStart < total; batchStart += batchSize {
		batchEnd := min(batchStart+batchSize, total)
		count := batchEnd - batchStart

		var wpSB strings.Builder
		wpSB.WriteString("INSERT INTO wallpapers (user_id, category_id, title, description, object_key, width, height, orientation, is_4k, size_bytes, status, created_at) VALUES ")
		wpArgs := make([]any, 0, count*7)

		var imgSB strings.Builder
		imgSB.WriteString("INSERT INTO wallpaper_images (wallpaper_id, sort_no, object_key, width, height, orientation, is_4k, size_bytes, created_at) VALUES ")
		imgArgs := make([]any, 0, count*3)

		var statsSB strings.Builder
		statsSB.WriteString("INSERT INTO wallpaper_stats (wallpaper_id, like_count, fav_count, download_count, comment_count, updated_at) VALUES ")
		statsArgs := make([]any, 0, count*6)

		var wtSB strings.Builder
		wtSB.WriteString("INSERT INTO wallpaper_tags (wallpaper_id, tag_id) VALUES ")
		wtArgs := make([]any, 0, count*4)
		wtFirst := true

		for i := batchStart; i < batchEnd; i++ {
			wpID := uint64(i + 1) // TRUNCATE 后自增从 1 严格连续
			// 真实社区分布：85% 来自全站其他 1050 名普通创作者，10% 来自 160 名被关注池普通作者，5% 来自 10 名大 V；
			// 使测试用户（关注 142 普通作者 + 8 大 V）的关注作品占比 ~13%，避免 IN 查询退化为只扫 20 行。
			var authorID uint64
			mod := i % 20
			switch {
			case mod == 0:
				authorID = meta.BigVAuthors[(i/20)%len(meta.BigVAuthors)]
			case mod == 1 || mod == 2:
				authorID = meta.NormalAuthors[(i/10)%len(meta.NormalAuthors)]
			default:
				authorID = meta.FanUserIDs[i%len(meta.FanUserIDs)]
			}
			catID := categoryIDs[i%len(categoryIDs)]
			createdAt := startTime.Add(step * time.Duration(i)).Format("2006-01-02 15:04:05.000")
			objKey := fmt.Sprintf("bench/wp/%06d.jpg", wpID)

			if i > batchStart {
				wpSB.WriteByte(',')
				imgSB.WriteByte(',')
				statsSB.WriteByte(',')
			}
			wpSB.WriteString("(?, ?, ?, 'bench wallpaper description', ?, 3840, 2160, 1, 1, 4200000, 1, ?)")
			wpArgs = append(wpArgs, authorID, catID, fmt.Sprintf("Bench 4K Wallpaper #%06d", wpID), objKey, createdAt)

			imgSB.WriteString("(?, 0, ?, 3840, 2160, 1, 1, 4200000, ?)")
			imgArgs = append(imgArgs, wpID, objKey, createdAt)

			// 幂律分布互动计数，制造真实头部热门作品
			rank := total - i // 越新编号越小
			likes := uint64(5 + (i % 40))
			favs := uint64(2 + (i % 20))
			dls := uint64(10 + (i % 80))
			cmts := uint64(i % 12)
			if rank <= 300 {
				boost := uint64(math.Round(3000.0 / math.Sqrt(float64(rank))))
				likes += boost
				favs += boost / 2
				dls += boost * 2
				cmts += boost / 10
			}
			statsSB.WriteString("(?, ?, ?, ?, ?, ?)")
			statsArgs = append(statsArgs, wpID, likes, favs, dls, cmts, createdAt)

			if len(tagIDs) >= 2 {
				t1 := tagIDs[i%len(tagIDs)]
				t2 := tagIDs[(i+7)%len(tagIDs)]
				if !wtFirst {
					wtSB.WriteByte(',')
				}
				wtFirst = false
				wtSB.WriteString("(?, ?),(?, ?)")
				wtArgs = append(wtArgs, wpID, t1, wpID, t2)
			}
		}

		if err := db.WithContext(ctx).Exec(wpSB.String(), wpArgs...).Error; err != nil {
			return fmt.Errorf("batch insert wallpapers [%d..%d]: %w", batchStart, batchEnd, err)
		}
		if err := db.WithContext(ctx).Exec(imgSB.String(), imgArgs...).Error; err != nil {
			return fmt.Errorf("batch insert wallpaper_images [%d..%d]: %w", batchStart, batchEnd, err)
		}
		if err := db.WithContext(ctx).Exec(statsSB.String(), statsArgs...).Error; err != nil {
			return fmt.Errorf("batch insert wallpaper_stats [%d..%d]: %w", batchStart, batchEnd, err)
		}
		if !wtFirst {
			if err := db.WithContext(ctx).Exec(wtSB.String(), wtArgs...).Error; err != nil {
				return fmt.Errorf("batch insert wallpaper_tags [%d..%d]: %w", batchStart, batchEnd, err)
			}
		}
	}
	return nil
}

func loadUserGroups(ctx context.Context, db *gorm.DB) (*SeedMeta, error) {
	type uRow struct {
		ID       uint64
		Username string
	}
	var rows []uRow
	if err := db.WithContext(ctx).Table("users").Select("id, username").Order("id ASC").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("load users: %w", err)
	}
	meta := &SeedMeta{TotalUsers: int64(len(rows))}
	for _, r := range rows {
		switch {
		case r.Username == "bench_reader":
			meta.ReaderID = r.ID
		case strings.HasPrefix(r.Username, "bench_fan_"):
			meta.FanUserIDs = append(meta.FanUserIDs, r.ID)
		case strings.HasPrefix(r.Username, "bench_author_"):
			meta.NormalAuthors = append(meta.NormalAuthors, r.ID)
		case strings.HasPrefix(r.Username, "bench_bigv_"):
			meta.BigVAuthors = append(meta.BigVAuthors, r.ID)
		}
	}
	if meta.ReaderID == 0 || len(meta.BigVAuthors) == 0 || len(meta.NormalAuthors) == 0 {
		return nil, fmt.Errorf("bench users not found, please run 'seed' first")
	}
	return meta, nil
}

func loadSeedMeta(ctx context.Context, db *gorm.DB) (*SeedMeta, error) {
	meta, err := loadUserGroups(ctx, db)
	if err != nil {
		return nil, err
	}

	if err := db.WithContext(ctx).Table("wallpapers").Count(&meta.TotalWallpapers).Error; err != nil {
		return nil, err
	}
	if err := db.WithContext(ctx).Table("user_follows").Count(&meta.TotalFollows).Error; err != nil {
		return nil, err
	}
	if err := db.WithContext(ctx).Table("user_follows").
		Select("followee_id").
		Where("follower_id = ?", meta.ReaderID).
		Order("followee_id ASC").
		Pluck("followee_id", &meta.FollowedIDs).Error; err != nil {
		return nil, err
	}

	// 取最新 200 条壁纸作为热点读集合
	if err := db.WithContext(ctx).Table("wallpapers").
		Select("id").
		Where("status = 1").
		Order("id DESC").
		Limit(200).
		Pluck("id", &meta.WallpaperIDs).Error; err != nil {
		return nil, err
	}
	if len(meta.WallpaperIDs) < 2 {
		return nil, fmt.Errorf("not enough wallpapers seeded")
	}
	meta.HotWallpaperID = meta.WallpaperIDs[0]

	// 取最早 1 条作为冷门压测单帖（用于 singleflight 击穿测试）
	if err := db.WithContext(ctx).Table("wallpapers").
		Select("id").
		Where("status = 1").
		Order("id ASC").
		Limit(1).
		Pluck("id", &meta.ColdWallpaper).Error; err != nil {
		return nil, err
	}
	return meta, nil
}
