package handler

import (
	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

// FeedHandler 处理推拉结合关注流与后续信息流 HTTP 接口。
type FeedHandler struct {
	feedSvc *service.FeedService
}

// NewFeedHandler 构造信息流处理器。
func NewFeedHandler(feedSvc *service.FeedService) *FeedHandler {
	return &FeedHandler{feedSvc: feedSvc}
}

// ListFollowingFeed 处理 GET /api/v1/feed/following（AuthRequired）。
func (h *FeedHandler) ListFollowingFeed(c *gin.Context) {
	limit, err := cursor.ParseLimit(c.Query("limit"), cursor.DefaultLimit, cursor.MaxLimit)
	if err != nil {
		response.Fail(c, err)
		return
	}

	userID := middleware.GetUserID(c)
	res, err := h.feedSvc.ListFollowingFeed(c.Request.Context(), userID, c.Query("cursor"), limit)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}
