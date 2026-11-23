package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/mq"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/repository"
)

const (
	maxCommentRunes           = 500
	interactionPublishTimeout = 200 * time.Millisecond
)

// InteractionService 统一收口 M6 点赞/收藏（参数化共用）、原图下载上报与一级评论业务。
type InteractionService struct {
	interactionRepo *repository.InteractionRepository
	wallpaperSvc    *WallpaperService
	userSvc         *UserService
	lightPub        mq.LightPublisher
}

// NewInteractionService 构造互动领域服务（lightPub 为 nil 时自动降级静默跳过 MQ 投递）。
func NewInteractionService(
	interactionRepo *repository.InteractionRepository,
	wallpaperSvc *WallpaperService,
	userSvc *UserService,
	lightPub mq.LightPublisher,
) *InteractionService {
	return &InteractionService{
		interactionRepo: interactionRepo,
		wallpaperSvc:    wallpaperSvc,
		userSvc:         userSvc,
		lightPub:        lightPub,
	}
}

// Like 点赞壁纸（严格幂等，仅真实新增时异步投递 +1 聚合事件）。
func (s *InteractionService) Like(ctx context.Context, userID, wallpaperID uint64) (*model.LikeActionResult, error) {
	if err := s.setRelation(ctx, model.InteractionKindLike, userID, wallpaperID, true); err != nil {
		return nil, err
	}
	return &model.LikeActionResult{IsLiked: true}, nil
}

// Unlike 取消点赞壁纸（严格幂等，允许清理已软删壁纸残留关系，仅真实删除时投递 -1）。
func (s *InteractionService) Unlike(ctx context.Context, userID, wallpaperID uint64) (*model.LikeActionResult, error) {
	if err := s.setRelation(ctx, model.InteractionKindLike, userID, wallpaperID, false); err != nil {
		return nil, err
	}
	return &model.LikeActionResult{IsLiked: false}, nil
}

// Favorite 收藏壁纸（严格幂等，仅真实新增时异步投递 +1 聚合事件）。
func (s *InteractionService) Favorite(ctx context.Context, userID, wallpaperID uint64) (*model.FavoriteActionResult, error) {
	if err := s.setRelation(ctx, model.InteractionKindFavorite, userID, wallpaperID, true); err != nil {
		return nil, err
	}
	return &model.FavoriteActionResult{IsFavorited: true}, nil
}

// Unfavorite 取消收藏壁纸（严格幂等，允许清理已软删壁纸残留关系，仅真实删除时投递 -1）。
func (s *InteractionService) Unfavorite(ctx context.Context, userID, wallpaperID uint64) (*model.FavoriteActionResult, error) {
	if err := s.setRelation(ctx, model.InteractionKindFavorite, userID, wallpaperID, false); err != nil {
		return nil, err
	}
	return &model.FavoriteActionResult{IsFavorited: false}, nil
}

// ListMyLikes 返回当前登录用户的“我赞过的”壁纸列表（按点赞关系主键倒序游标分页）。
func (s *InteractionService) ListMyLikes(ctx context.Context, userID uint64, rawCursor string, rawLimit int) (*response.PageData[model.WallpaperSummary], error) {
	return s.listMyRelationWallpapers(ctx, model.InteractionKindLike, userID, rawCursor, rawLimit)
}

// ListMyFavorites 返回当前登录用户的“我的收藏”壁纸列表（按收藏关系主键倒序游标分页）。
func (s *InteractionService) ListMyFavorites(ctx context.Context, userID uint64, rawCursor string, rawLimit int) (*response.PageData[model.WallpaperSummary], error) {
	return s.listMyRelationWallpapers(ctx, model.InteractionKindFavorite, userID, rawCursor, rawLimit)
}

// RecordDownload 受理原图下载计数上报（允许游客与登录用户，0 次 DB 写，轻量直发 MQ 削峰）。
func (s *InteractionService) RecordDownload(ctx context.Context, userID, wallpaperID uint64) error {
	if err := s.wallpaperSvc.ensureWallpaperActive(ctx, wallpaperID); err != nil {
		return err
	}
	s.publishInteraction(ctx, wallpaperID, userID, model.InteractionActionDownload, 1)
	return nil
}

// CreateComment 发表一级评论，同步落库后轻量投递 comment +1 聚合事件。
func (s *InteractionService) CreateComment(
	ctx context.Context,
	userID, wallpaperID uint64,
	req model.CreateCommentRequest,
) (*model.CommentItem, error) {
	content, err := validateCommentContent(req.Content)
	if err != nil {
		return nil, err
	}
	if err := s.wallpaperSvc.ensureWallpaperActive(ctx, wallpaperID); err != nil {
		return nil, err
	}

	comment := &model.Comment{
		WallpaperID: wallpaperID,
		UserID:      userID,
		Content:     content,
		Status:      model.CommentStatusActive,
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.interactionRepo.CreateComment(ctx, comment); err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	s.publishInteraction(ctx, wallpaperID, userID, model.InteractionActionComment, 1)

	author, err := s.userSvc.GetAuthor(ctx, userID)
	if err != nil {
		return nil, err
	}
	return &model.CommentItem{
		ID:          comment.ID,
		WallpaperID: comment.WallpaperID,
		Content:     comment.Content,
		Author:      author,
		CreatedAt:   comment.CreatedAt.UTC(),
	}, nil
}

// ListComments 按评论主键倒序游标分页返回壁纸一级评论列表（批量装配评论作者）。
func (s *InteractionService) ListComments(
	ctx context.Context,
	wallpaperID uint64,
	rawCursor string,
	rawLimit int,
) (*response.PageData[model.CommentItem], error) {
	limit, err := normalizeListLimit(rawLimit)
	if err != nil {
		return nil, err
	}
	beforeID, err := parseIDCursor(rawCursor)
	if err != nil {
		return nil, err
	}
	if err := s.wallpaperSvc.ensureWallpaperActive(ctx, wallpaperID); err != nil {
		return nil, err
	}

	rows, err := s.interactionRepo.ListCommentsByWallpaper(ctx, wallpaperID, beforeID, limit+1)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	hasMore := len(rows) > limit
	var nextCursor string
	if hasMore {
		rows = rows[:limit]
		nextCursor, err = cursor.Encode(cursor.IDCursor{ID: rows[len(rows)-1].ID})
		if err != nil {
			return nil, errno.ErrInternal.Wrap(err)
		}
	}
	if len(rows) == 0 {
		page := response.NewPageData[model.CommentItem](nil, "", false)
		return &page, nil
	}

	userIDs := make([]uint64, 0, len(rows))
	seen := make(map[uint64]struct{}, len(rows))
	for i := range rows {
		uid := rows[i].UserID
		if _, dup := seen[uid]; dup {
			continue
		}
		seen[uid] = struct{}{}
		userIDs = append(userIDs, uid)
	}
	authors, err := s.userSvc.GetAuthorsByIDs(ctx, userIDs)
	if err != nil {
		return nil, err
	}

	items := make([]model.CommentItem, 0, len(rows))
	for i := range rows {
		c := rows[i]
		author, ok := authors[c.UserID]
		if !ok {
			author = model.WallpaperAuthor{ID: c.UserID, AvatarURL: s.userSvc.BuildAvatarURL("")}
		}
		items = append(items, model.CommentItem{
			ID:          c.ID,
			WallpaperID: c.WallpaperID,
			Content:     c.Content,
			Author:      author,
			CreatedAt:   c.CreatedAt.UTC(),
		})
	}
	page := response.NewPageData(items, nextCursor, hasMore)
	return &page, nil
}

// DeleteComment 软删一级评论（仅评论作者或所属壁纸主人有权删除，原子条件更新防并发双扣）。
func (s *InteractionService) DeleteComment(ctx context.Context, userID, commentID uint64) error {
	rec, err := s.interactionRepo.FindCommentDeleteRecord(ctx, commentID)
	if err != nil {
		if errors.Is(err, repository.ErrCommentNotFound) {
			return errno.ErrCommentNotFound
		}
		return errno.ErrInternal.Wrap(err)
	}
	if rec.Status != model.CommentStatusActive {
		return errno.ErrCommentNotFound
	}

	if rec.UserID != userID {
		ownerID, err := s.wallpaperSvc.findWallpaperOwnerID(ctx, rec.WallpaperID)
		if err != nil {
			return err
		}
		if ownerID != userID {
			return errno.ErrForbidden
		}
	}

	changed, err := s.interactionRepo.SoftDeleteComment(ctx, commentID)
	if err != nil {
		return errno.ErrInternal.Wrap(err)
	}
	if !changed {
		return errno.ErrCommentNotFound
	}

	s.publishInteraction(ctx, rec.WallpaperID, userID, model.InteractionActionComment, -1)
	return nil
}

func (s *InteractionService) setRelation(ctx context.Context, kind model.InteractionKind, userID, wallpaperID uint64, add bool) error {
	var (
		changed bool
		err     error
		delta   int64 = -1
	)
	if add {
		if err := s.wallpaperSvc.ensureWallpaperActive(ctx, wallpaperID); err != nil {
			return err
		}
		changed, err = s.interactionRepo.AddRelation(ctx, kind, userID, wallpaperID)
		delta = 1
	} else {
		changed, err = s.interactionRepo.RemoveRelation(ctx, kind, userID, wallpaperID)
	}
	if err != nil {
		return errno.ErrInternal.Wrap(err)
	}
	if changed {
		s.publishInteraction(ctx, wallpaperID, userID, interactionAction(kind), delta)
	}
	return nil
}

func (s *InteractionService) listMyRelationWallpapers(
	ctx context.Context,
	kind model.InteractionKind,
	userID uint64,
	rawCursor string,
	rawLimit int,
) (*response.PageData[model.WallpaperSummary], error) {
	limit, err := normalizeListLimit(rawLimit)
	if err != nil {
		return nil, err
	}
	beforeID, err := parseIDCursor(rawCursor)
	if err != nil {
		return nil, err
	}

	edges, err := s.interactionRepo.ListUserRelationPage(ctx, kind, userID, beforeID, limit+1)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	hasMore := len(edges) > limit
	var nextCursor string
	if hasMore {
		edges = edges[:limit]
		nextCursor, err = cursor.Encode(cursor.IDCursor{ID: edges[len(edges)-1].ID})
		if err != nil {
			return nil, errno.ErrInternal.Wrap(err)
		}
	}

	wpIDs := make([]uint64, 0, len(edges))
	for _, e := range edges {
		wpIDs = append(wpIDs, e.WallpaperID)
	}
	return s.wallpaperSvc.assembleSummaryPage(ctx, wpIDs, nextCursor, hasMore, summaryAssembleOpts{
		viewerID:       userID,
		knownLiked:     kind == model.InteractionKindLike,
		knownFavorited: kind == model.InteractionKindFavorite,
	})
}

func (s *InteractionService) publishInteraction(ctx context.Context, wallpaperID, userID uint64, action string, delta int64) {
	if s.lightPub == nil || wallpaperID == 0 {
		return
	}
	ev := model.NewInteractionEvent(wallpaperID, userID, action, delta, time.Now())
	body, err := json.Marshal(ev)
	if err != nil {
		slog.WarnContext(ctx, "marshal interaction event failed",
			slog.Uint64("wallpaper_id", wallpaperID),
			slog.String("action", action),
			slog.Any("err", err),
		)
		return
	}

	pubCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), interactionPublishTimeout)
	defer cancel()

	if err := s.lightPub.PublishLight(pubCtx, mq.PublishMessage{
		Exchange:   mq.ExchangeEvents,
		RoutingKey: mq.RoutingKeyWallpaperInteraction,
		MessageID:  ev.EventID,
		Body:       body,
	}); err != nil {
		slog.WarnContext(ctx, "publish interaction event degraded",
			slog.Uint64("wallpaper_id", wallpaperID),
			slog.String("action", action),
			slog.Int64("delta", delta),
			slog.Any("err", err),
		)
	}
}

func interactionAction(kind model.InteractionKind) string {
	if kind == model.InteractionKindFavorite {
		return model.InteractionActionFavorite
	}
	return model.InteractionActionLike
}

func validateCommentContent(raw string) (string, error) {
	content := strings.TrimSpace(raw)
	n := utf8.RuneCountInString(content)
	if n < 1 || n > maxCommentRunes {
		return "", errno.ErrInvalidComment.WithMessage("评论内容长度需为 1~500 个字符")
	}
	return content, nil
}
