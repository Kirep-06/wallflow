package handler

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

type FeedHandler struct {
	latest    *service.FeedService
	following *service.FollowingFeedService
}

func NewFeedHandler(latest *service.FeedService, following *service.FollowingFeedService) *FeedHandler {
	return &FeedHandler{latest: latest, following: following}
}

func (h *FeedHandler) Latest(c *gin.Context) {
	var limit int
	if raw := c.Query("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			response.Error(c, errno.ErrParam)
			return
		}
		limit = v
	}
	page, err := h.latest.Latest(c.Request.Context(), c.Query("cursor"), limit)
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, page)
}

func (h *FeedHandler) Following(c *gin.Context) {
	var limit int
	if c.Request.URL.Query().Has("limit") {
		v, err := strconv.Atoi(c.Query("limit"))
		if err != nil || v < 1 || v > h.following.PageMax() {
			response.Error(c, errno.ErrParam)
			return
		}
		limit = v
	}
	page, err := h.following.Following(c.Request.Context(), middleware.UserID(c), c.Query("cursor"), limit)
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, page)
}
