package handler

import (
	"context"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

// InteractionHandler 统一收口 M6 点赞/收藏、我的列表、原图下载上报与一级评论 HTTP 接口。
type InteractionHandler struct {
	interactionSvc *service.InteractionService
}

// NewInteractionHandler 构造互动处理器。
func NewInteractionHandler(interactionSvc *service.InteractionService) *InteractionHandler {
	return &InteractionHandler{interactionSvc: interactionSvc}
}

// Like 处理 POST /api/v1/wallpapers/:id/like。
func (h *InteractionHandler) Like(c *gin.Context) {
	h.handleWallpaperAction(c, func(ctx context.Context, uid, wpID uint64) (any, error) {
		return h.interactionSvc.Like(ctx, uid, wpID)
	})
}

// Unlike 处理 DELETE /api/v1/wallpapers/:id/like。
func (h *InteractionHandler) Unlike(c *gin.Context) {
	h.handleWallpaperAction(c, func(ctx context.Context, uid, wpID uint64) (any, error) {
		return h.interactionSvc.Unlike(ctx, uid, wpID)
	})
}

// Favorite 处理 POST /api/v1/wallpapers/:id/favorite。
func (h *InteractionHandler) Favorite(c *gin.Context) {
	h.handleWallpaperAction(c, func(ctx context.Context, uid, wpID uint64) (any, error) {
		return h.interactionSvc.Favorite(ctx, uid, wpID)
	})
}

// Unfavorite 处理 DELETE /api/v1/wallpapers/:id/favorite。
func (h *InteractionHandler) Unfavorite(c *gin.Context) {
	h.handleWallpaperAction(c, func(ctx context.Context, uid, wpID uint64) (any, error) {
		return h.interactionSvc.Unfavorite(ctx, uid, wpID)
	})
}

// RecordDownload 处理 POST /api/v1/wallpapers/:id/download（AuthOptional）。
func (h *InteractionHandler) RecordDownload(c *gin.Context) {
	h.handleWallpaperAction(c, func(ctx context.Context, uid, wpID uint64) (any, error) {
		return nil, h.interactionSvc.RecordDownload(ctx, uid, wpID)
	})
}

// ListMyFavorites 处理 GET /api/v1/users/me/favorites。
func (h *InteractionHandler) ListMyFavorites(c *gin.Context) {
	h.handleMyRelationList(c, h.interactionSvc.ListMyFavorites)
}

// ListMyLikes 处理 GET /api/v1/users/me/likes。
func (h *InteractionHandler) ListMyLikes(c *gin.Context) {
	h.handleMyRelationList(c, h.interactionSvc.ListMyLikes)
}

// ListComments 处理 GET /api/v1/wallpapers/:id/comments。
func (h *InteractionHandler) ListComments(c *gin.Context) {
	wallpaperID, err := parsePositiveUint64(c.Param("id"), "壁纸 ID 格式不合法")
	if err != nil {
		response.Fail(c, err)
		return
	}
	limit, err := cursor.ParseLimit(c.Query("limit"), cursor.DefaultLimit, cursor.MaxLimit)
	if err != nil {
		response.Fail(c, err)
		return
	}

	res, err := h.interactionSvc.ListComments(c.Request.Context(), wallpaperID, c.Query("cursor"), limit)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

// CreateComment 处理 POST /api/v1/wallpapers/:id/comments。
func (h *InteractionHandler) CreateComment(c *gin.Context) {
	wallpaperID, err := parsePositiveUint64(c.Param("id"), "壁纸 ID 格式不合法")
	if err != nil {
		response.Fail(c, err)
		return
	}
	var req model.CreateCommentRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		response.Fail(c, err)
		return
	}

	res, err := h.interactionSvc.CreateComment(c.Request.Context(), middleware.GetUserID(c), wallpaperID, req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.Created(c, res)
}

// DeleteComment 处理 DELETE /api/v1/comments/:id。
func (h *InteractionHandler) DeleteComment(c *gin.Context) {
	commentID, err := parsePositiveUint64(c.Param("id"), "评论 ID 格式不合法")
	if err != nil {
		response.Fail(c, err)
		return
	}
	if err := h.interactionSvc.DeleteComment(c.Request.Context(), middleware.GetUserID(c), commentID); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, nil)
}

func (h *InteractionHandler) handleWallpaperAction(
	c *gin.Context,
	action func(ctx context.Context, userID, wallpaperID uint64) (any, error),
) {
	wallpaperID, err := parsePositiveUint64(c.Param("id"), "壁纸 ID 格式不合法")
	if err != nil {
		response.Fail(c, err)
		return
	}
	res, err := action(c.Request.Context(), middleware.GetUserID(c), wallpaperID)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

func (h *InteractionHandler) handleMyRelationList(
	c *gin.Context,
	listFn func(ctx context.Context, userID uint64, rawCursor string, rawLimit int) (*response.PageData[model.WallpaperSummary], error),
) {
	limit, err := cursor.ParseLimit(c.Query("limit"), cursor.DefaultLimit, cursor.MaxLimit)
	if err != nil {
		response.Fail(c, err)
		return
	}
	res, err := listFn(c.Request.Context(), middleware.GetUserID(c), c.Query("cursor"), limit)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}
