package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

// FeedHandler 处理推拉结合关注流与热门榜 HTTP 接口。
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

// ListHotFeed 处理 GET /api/v1/feed/hot（AuthOptional，支持按 tag_id 查单标签 TopN 热榜及全站首屏弱 ETag / 304 协商）。
func (h *FeedHandler) ListHotFeed(c *gin.Context) {
	limit, err := cursor.ParseLimit(c.Query("limit"), cursor.DefaultLimit, cursor.MaxLimit)
	if err != nil {
		response.Fail(c, err)
		return
	}

	viewerID := middleware.GetUserID(c)
	if rawTag := c.Query("tag_id"); rawTag != "" {
		tagID, parseErr := parsePositiveUint64(rawTag, "tag_id 参数非法")
		if parseErr != nil {
			response.Fail(c, parseErr)
			return
		}
		page, tagErr := h.feedSvc.ListTagHotFeed(c.Request.Context(), viewerID, tagID, limit)
		if tagErr != nil {
			response.Fail(c, tagErr)
			return
		}
		c.Header("Cache-Control", "private, no-store")
		response.OK(c, page)
		return
	}

	res, err := h.feedSvc.ListHotFeed(c.Request.Context(), viewerID, c.Query("cursor"), limit, c.GetHeader("If-None-Match"))
	if err != nil {
		response.Fail(c, err)
		return
	}

	if res.ETag != "" {
		c.Header("ETag", res.ETag)
		c.Header("Cache-Control", "private, no-cache")
		c.Writer.Header().Add("Vary", "Authorization")
		if res.NotModified {
			c.Status(http.StatusNotModified)
			return
		}
	} else {
		c.Header("Cache-Control", "private, no-store")
	}

	response.OK(c, res.Page)
}
