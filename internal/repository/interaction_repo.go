package repository

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"gorm.io/gorm"

	"wallpaper-feed/internal/model"
)

// ErrCommentNotFound 表示评论不存在或已软删。
var ErrCommentNotFound = errors.New("repository: comment not found")

// InteractionRepository 统一收口 wallpaper_stats、wallpaper_likes、wallpaper_favorites 与 comments 四张表的数据访问。
type InteractionRepository struct {
	db *gorm.DB
}

// NewInteractionRepository 构造互动领域仓储实例。
func NewInteractionRepository(db *gorm.DB) *InteractionRepository {
	return &InteractionRepository{db: db}
}

func relationTable(kind model.InteractionKind) (string, error) {
	switch kind {
	case model.InteractionKindLike:
		return model.WallpaperLike{}.TableName(), nil
	case model.InteractionKindFavorite:
		return model.WallpaperFavorite{}.TableName(), nil
	default:
		return "", fmt.Errorf("repository: invalid interaction kind %d", kind)
	}
}

// AddRelation 原子写入点赞或收藏关系；唯一键冲突返回 changed=false, err=nil，仅真实新增返回 changed=true。
func (r *InteractionRepository) AddRelation(ctx context.Context, kind model.InteractionKind, userID, wallpaperID uint64) (bool, error) {
	now := time.Now().UTC()
	var err error
	switch kind {
	case model.InteractionKindLike:
		err = r.db.WithContext(ctx).Create(&model.WallpaperLike{
			UserID:      userID,
			WallpaperID: wallpaperID,
			CreatedAt:   now,
		}).Error
	case model.InteractionKindFavorite:
		err = r.db.WithContext(ctx).Create(&model.WallpaperFavorite{
			UserID:      userID,
			WallpaperID: wallpaperID,
			CreatedAt:   now,
		}).Error
	default:
		return false, fmt.Errorf("repository: invalid interaction kind %d", kind)
	}
	if err != nil {
		if isDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("interaction repo add relation(%d): %w", kind, err)
	}
	return true, nil
}

// RemoveRelation 命中 uk_user_wp 物理删除点赞或收藏关系，仅真实删除行返回 changed=true。
func (r *InteractionRepository) RemoveRelation(ctx context.Context, kind model.InteractionKind, userID, wallpaperID uint64) (bool, error) {
	table, err := relationTable(kind)
	if err != nil {
		return false, err
	}
	res := r.db.WithContext(ctx).Exec(
		"DELETE FROM "+table+" WHERE user_id = ? AND wallpaper_id = ?",
		userID, wallpaperID,
	)
	if res.Error != nil {
		return false, fmt.Errorf("interaction repo remove relation(%d): %w", kind, res.Error)
	}
	return res.RowsAffected > 0, nil
}

// ListUserRelationPage 命中 idx_user_list 覆盖索引零回表倒序查询用户的赞过/收藏关系边。
func (r *InteractionRepository) ListUserRelationPage(
	ctx context.Context,
	kind model.InteractionKind,
	userID, beforeID uint64,
	limit int,
) ([]model.InteractionEdge, error) {
	if limit <= 0 {
		return []model.InteractionEdge{}, nil
	}
	table, err := relationTable(kind)
	if err != nil {
		return nil, err
	}

	q := r.db.WithContext(ctx).
		Table(table).
		Select("id, wallpaper_id").
		Where("user_id = ?", userID)
	if beforeID > 0 {
		q = q.Where("id < ?", beforeID)
	}

	var edges []model.InteractionEdge
	if err := q.Order("id DESC").Limit(limit).Scan(&edges).Error; err != nil {
		return nil, fmt.Errorf("interaction repo list relation(%d): %w", kind, err)
	}
	return edges, nil
}

// BatchCheckRelations 命中 uk_user_wp 覆盖索引批量判断用户对指定壁纸集合的点赞或收藏状态。
func (r *InteractionRepository) BatchCheckRelations(
	ctx context.Context,
	kind model.InteractionKind,
	userID uint64,
	wallpaperIDs []uint64,
) (map[uint64]struct{}, error) {
	out := make(map[uint64]struct{})
	if userID == 0 || len(wallpaperIDs) == 0 {
		return out, nil
	}
	table, err := relationTable(kind)
	if err != nil {
		return nil, err
	}

	var hitIDs []uint64
	if err := r.db.WithContext(ctx).
		Table(table).
		Select("wallpaper_id").
		Where("user_id = ? AND wallpaper_id IN ?", userID, wallpaperIDs).
		Pluck("wallpaper_id", &hitIDs).Error; err != nil {
		return nil, fmt.Errorf("interaction repo batch check(%d): %w", kind, err)
	}
	for _, id := range hitIDs {
		out[id] = struct{}{}
	}
	return out, nil
}

// CreateComment 写入一级评论记录。
func (r *InteractionRepository) CreateComment(ctx context.Context, c *model.Comment) error {
	if err := r.db.WithContext(ctx).Create(c).Error; err != nil {
		return fmt.Errorf("interaction repo create comment: %w", err)
	}
	return nil
}

// ListCommentsByWallpaper 命中 idx_wp_status_id 倒序游标分页查询壁纸正常评论。
func (r *InteractionRepository) ListCommentsByWallpaper(
	ctx context.Context,
	wallpaperID, beforeID uint64,
	limit int,
) ([]model.Comment, error) {
	if limit <= 0 {
		return []model.Comment{}, nil
	}
	q := r.db.WithContext(ctx).
		Select("id, wallpaper_id, user_id, content, created_at").
		Where("wallpaper_id = ? AND status = ?", wallpaperID, model.CommentStatusActive)
	if beforeID > 0 {
		q = q.Where("id < ?", beforeID)
	}

	var rows []model.Comment
	if err := q.Order("id DESC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("interaction repo list comments: %w", err)
	}
	return rows, nil
}

// FindCommentDeleteRecord 查询评论软删鉴权所需的最小字段投影。
func (r *InteractionRepository) FindCommentDeleteRecord(ctx context.Context, commentID uint64) (*model.CommentDeleteRecord, error) {
	var rec model.CommentDeleteRecord
	err := r.db.WithContext(ctx).
		Table(model.Comment{}.TableName()).
		Select("id, wallpaper_id, user_id, status").
		Where("id = ?", commentID).
		Take(&rec).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCommentNotFound
		}
		return nil, fmt.Errorf("interaction repo find comment delete record: %w", err)
	}
	return &rec, nil
}

// SoftDeleteComment 执行原子条件软删（WHERE id = ? AND status = 1），仅真实变更返回 changed=true。
func (r *InteractionRepository) SoftDeleteComment(ctx context.Context, commentID uint64) (bool, error) {
	res := r.db.WithContext(ctx).
		Model(&model.Comment{}).
		Where("id = ? AND status = ?", commentID, model.CommentStatusActive).
		Update("status", model.CommentStatusDeleted)
	if res.Error != nil {
		return false, fmt.Errorf("interaction repo soft delete comment: %w", res.Error)
	}
	return res.RowsAffected == 1, nil
}

// FindStatsByWallpaperID 查询单张壁纸互动统计；懒初始化查无行返回全零结构体。
func (r *InteractionRepository) FindStatsByWallpaperID(ctx context.Context, wallpaperID uint64) (model.WallpaperStatsCache, error) {
	var row model.WallpaperStats
	err := r.db.WithContext(ctx).
		Select("wallpaper_id, like_count, fav_count, download_count, comment_count").
		Where("wallpaper_id = ?", wallpaperID).
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.WallpaperStatsCache{}, nil
	}
	if err != nil {
		return model.WallpaperStatsCache{}, fmt.Errorf("interaction repo find stats by id: %w", err)
	}
	return row.ToStatsCache(), nil
}

// FindStatsByWallpaperIDs 批量查询壁纸互动统计；未初始化行自动补全零结构体防缓存穿透。
func (r *InteractionRepository) FindStatsByWallpaperIDs(ctx context.Context, wallpaperIDs []uint64) (map[uint64]model.WallpaperStatsCache, error) {
	out := make(map[uint64]model.WallpaperStatsCache, len(wallpaperIDs))
	if len(wallpaperIDs) == 0 {
		return out, nil
	}
	for _, id := range wallpaperIDs {
		out[id] = model.WallpaperStatsCache{}
	}

	var rows []model.WallpaperStats
	if err := r.db.WithContext(ctx).
		Select("wallpaper_id, like_count, fav_count, download_count, comment_count").
		Where("wallpaper_id IN ?", wallpaperIDs).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("interaction repo find stats by ids: %w", err)
	}
	for _, row := range rows {
		out[row.WallpaperID] = row.ToStatsCache()
	}
	return out, nil
}

// BatchApplyStatsDeltas 按 wallpaper_id ASC 升序执行单条防 1264/1690 下溢与防 1213 死锁的批量刷盘 SQL。
func (r *InteractionRepository) BatchApplyStatsDeltas(ctx context.Context, deltas []model.StatsDelta) error {
	merged := normalizeStatsDeltas(deltas)
	if len(merged) == 0 {
		return nil
	}

	sqlStr, args := buildBatchStatsUpsertSQL(merged)
	if err := r.db.WithContext(ctx).Exec(sqlStr, args...).Error; err != nil {
		return fmt.Errorf("interaction repo batch apply stats deltas: %w", err)
	}
	return nil
}

// FindStatsReconcileRows 批量读取对账所需的现有统计值与 updated_at 乐观锁时间戳。
func (r *InteractionRepository) FindStatsReconcileRows(ctx context.Context, wallpaperIDs []uint64) (map[uint64]model.StatsReconcileRow, error) {
	out := make(map[uint64]model.StatsReconcileRow, len(wallpaperIDs))
	if len(wallpaperIDs) == 0 {
		return out, nil
	}

	var rows []model.StatsReconcileRow
	if err := r.db.WithContext(ctx).
		Table(model.WallpaperStats{}.TableName()).
		Select("wallpaper_id, like_count, fav_count, comment_count, updated_at").
		Where("wallpaper_id IN ?", wallpaperIDs).
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("interaction repo find stats reconcile rows: %w", err)
	}
	for _, row := range rows {
		row.UpdatedAt = row.UpdatedAt.UTC()
		out[row.WallpaperID] = row
	}
	return out, nil
}

// CountExactInteractions 经 idx_wp 与 idx_wp_status_id 三条覆盖索引 SQL 统计壁纸真实点赞/收藏/评论数。
func (r *InteractionRepository) CountExactInteractions(
	ctx context.Context,
	wallpaperIDs []uint64,
) (likes, favs, comments map[uint64]uint64, err error) {
	likes = make(map[uint64]uint64)
	favs = make(map[uint64]uint64)
	comments = make(map[uint64]uint64)
	if len(wallpaperIDs) == 0 {
		return likes, favs, comments, nil
	}

	if likes, err = r.countByWallpaper(ctx, model.WallpaperLike{}.TableName(), wallpaperIDs, false); err != nil {
		return nil, nil, nil, err
	}
	if favs, err = r.countByWallpaper(ctx, model.WallpaperFavorite{}.TableName(), wallpaperIDs, false); err != nil {
		return nil, nil, nil, err
	}
	if comments, err = r.countByWallpaper(ctx, model.Comment{}.TableName(), wallpaperIDs, true); err != nil {
		return nil, nil, nil, err
	}
	return likes, favs, comments, nil
}

func (r *InteractionRepository) countByWallpaper(
	ctx context.Context,
	table string,
	wallpaperIDs []uint64,
	onlyActive bool,
) (map[uint64]uint64, error) {
	type countRow struct {
		WallpaperID uint64
		Total       uint64
	}
	q := r.db.WithContext(ctx).
		Table(table).
		Select("wallpaper_id, COUNT(*) AS total").
		Where("wallpaper_id IN ?", wallpaperIDs)
	if onlyActive {
		q = q.Where("status = ?", model.CommentStatusActive)
	}

	var rows []countRow
	if err := q.Group("wallpaper_id").Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("interaction repo count %s by wallpaper: %w", table, err)
	}
	out := make(map[uint64]uint64, len(rows))
	for _, row := range rows {
		out[row.WallpaperID] = row.Total
	}
	return out, nil
}

// InsertReconciledStats 补建因极端丢包而缺失的 wallpaper_stats 统计行（INSERT IGNORE 避免覆盖并发新建行）。
func (r *InteractionRepository) InsertReconciledStats(ctx context.Context, row model.StatsReconcileRow) (bool, error) {
	res := r.db.WithContext(ctx).Exec(
		"INSERT IGNORE INTO "+model.WallpaperStats{}.TableName()+
			" (wallpaper_id, like_count, fav_count, download_count, comment_count) VALUES (?, ?, ?, 0, ?)",
		row.WallpaperID, row.LikeCount, row.FavCount, row.CommentCount,
	)
	if res.Error != nil {
		return false, fmt.Errorf("interaction repo insert reconciled stats: %w", res.Error)
	}
	return res.RowsAffected == 1, nil
}

// CompareAndUpdateStats 基于 updated_at 乐观锁 CAS 修正三项可对账计数，保留 download_count 不变且不覆写在途热写。
func (r *InteractionRepository) CompareAndUpdateStats(
	ctx context.Context,
	wallpaperID, likes, favs, comments uint64,
	expectedUpdatedAt time.Time,
) (bool, error) {
	res := r.db.WithContext(ctx).Exec(
		"UPDATE "+model.WallpaperStats{}.TableName()+
			" SET like_count = ?, fav_count = ?, comment_count = ?, updated_at = CURRENT_TIMESTAMP(3)"+
			" WHERE wallpaper_id = ? AND updated_at = ?",
		likes, favs, comments, wallpaperID, expectedUpdatedAt.UTC(),
	)
	if res.Error != nil {
		return false, fmt.Errorf("interaction repo cas update stats: %w", res.Error)
	}
	return res.RowsAffected == 1, nil
}

func normalizeStatsDeltas(deltas []model.StatsDelta) []model.StatsDelta {
	if len(deltas) == 0 {
		return nil
	}
	agg := make(map[uint64]model.StatsDelta, len(deltas))
	for _, d := range deltas {
		if d.WallpaperID == 0 || d.IsZero() {
			continue
		}
		cur := agg[d.WallpaperID]
		cur.WallpaperID = d.WallpaperID
		cur.Like += d.Like
		cur.Fav += d.Fav
		cur.Download += d.Download
		cur.Comment += d.Comment
		agg[d.WallpaperID] = cur
	}
	out := make([]model.StatsDelta, 0, len(agg))
	for _, d := range agg {
		if !d.IsZero() {
			out = append(out, d)
		}
	}
	slices.SortFunc(out, func(a, b model.StatsDelta) int {
		return cmp.Compare(a.WallpaperID, b.WallpaperID)
	})
	return out
}

func buildBatchStatsUpsertSQL(deltas []model.StatsDelta) (string, []any) {
	var b strings.Builder
	b.Grow(512 + len(deltas)*48)
	b.WriteString("INSERT INTO ")
	b.WriteString(model.WallpaperStats{}.TableName())
	b.WriteString(" (wallpaper_id, like_count, fav_count, download_count, comment_count) ")
	b.WriteString("SELECT d.wallpaper_id, GREATEST(d.dl, 0), GREATEST(d.df, 0), GREATEST(d.dd, 0), GREATEST(d.dc, 0) FROM (")

	args := make([]any, 0, len(deltas)*5)
	for i, d := range deltas {
		if i == 0 {
			b.WriteString("SELECT CAST(? AS UNSIGNED) AS wallpaper_id, CAST(? AS SIGNED) AS dl, CAST(? AS SIGNED) AS df, CAST(? AS SIGNED) AS dd, CAST(? AS SIGNED) AS dc")
		} else {
			b.WriteString(" UNION ALL SELECT ?, ?, ?, ?, ?")
		}
		args = append(args, d.WallpaperID, d.Like, d.Fav, d.Download, d.Comment)
	}

	b.WriteString(") AS d ON DUPLICATE KEY UPDATE ")
	b.WriteString("like_count = IF(d.dl >= 0, like_count + d.dl, IF(like_count > CAST(-d.dl AS UNSIGNED), like_count - CAST(-d.dl AS UNSIGNED), 0)), ")
	b.WriteString("fav_count = IF(d.df >= 0, fav_count + d.df, IF(fav_count > CAST(-d.df AS UNSIGNED), fav_count - CAST(-d.df AS UNSIGNED), 0)), ")
	b.WriteString("download_count = download_count + GREATEST(d.dd, 0), ")
	b.WriteString("comment_count = IF(d.dc >= 0, comment_count + d.dc, IF(comment_count > CAST(-d.dc AS UNSIGNED), comment_count - CAST(-d.dc AS UNSIGNED), 0)), ")
	b.WriteString("updated_at = CURRENT_TIMESTAMP(3)")
	return b.String(), args
}
