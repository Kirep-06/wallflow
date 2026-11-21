package handler

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

type FeedHandler struct {
	svc *service.FeedService
}

func NewFeedHandler(svc *service.FeedService) *FeedHandler { return &FeedHandler{svc: svc} }

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
	page, err := h.svc.Latest(c.Request.Context(), c.Query("cursor"), limit)
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, page)
}
