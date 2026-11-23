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

// FollowHandler 处理 M4 关注/取关与关注/粉丝列表 HTTP 接口。
type FollowHandler struct {
	followSvc *service.FollowService
}

// NewFollowHandler 构造关注关系处理器。
func NewFollowHandler(followSvc *service.FollowService) *FollowHandler {
	return &FollowHandler{followSvc: followSvc}
}

// Follow 处理 POST /api/v1/users/:id/follow。
func (h *FollowHandler) Follow(c *gin.Context) {
	h.handleFollowAction(c, h.followSvc.Follow)
}

// Unfollow 处理 DELETE /api/v1/users/:id/follow。
func (h *FollowHandler) Unfollow(c *gin.Context) {
	h.handleFollowAction(c, h.followSvc.Unfollow)
}

// ListFollowing 处理 GET /api/v1/users/:id/following。
func (h *FollowHandler) ListFollowing(c *gin.Context) {
	h.handleFollowList(c, h.followSvc.ListFollowing)
}

// ListFollowers 处理 GET /api/v1/users/:id/followers。
func (h *FollowHandler) ListFollowers(c *gin.Context) {
	h.handleFollowList(c, h.followSvc.ListFollowers)
}

func (h *FollowHandler) handleFollowAction(
	c *gin.Context,
	action func(ctx context.Context, followerID, targetID uint64) (*model.FollowActionResult, error),
) {
	targetID, err := parsePositiveUint64(c.Param("id"), "用户 ID 格式不合法")
	if err != nil {
		response.Fail(c, err)
		return
	}

	followerID := middleware.GetUserID(c)
	res, err := action(c.Request.Context(), followerID, targetID)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

func (h *FollowHandler) handleFollowList(
	c *gin.Context,
	listFn func(ctx context.Context, targetUserID, viewerID uint64, rawCursor string, rawLimit int) (*response.PageData[model.FollowUserItem], error),
) {
	targetID, err := parsePositiveUint64(c.Param("id"), "用户 ID 格式不合法")
	if err != nil {
		response.Fail(c, err)
		return
	}
	limit, err := cursor.ParseLimit(c.Query("limit"), cursor.DefaultLimit, cursor.MaxLimit)
	if err != nil {
		response.Fail(c, err)
		return
	}

	viewerID := middleware.GetUserID(c)
	res, err := listFn(c.Request.Context(), targetID, viewerID, c.Query("cursor"), limit)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}
