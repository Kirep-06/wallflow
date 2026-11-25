package handler

import (
	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

// MetaHandler 处理标签公开查询接口。
type MetaHandler struct {
	wallpaperSvc *service.WallpaperService
}

// NewMetaHandler 构造标签处理器。
func NewMetaHandler(wallpaperSvc *service.WallpaperService) *MetaHandler {
	return &MetaHandler{wallpaperSvc: wallpaperSvc}
}

// ListTags 处理 GET /api/v1/tags。
func (h *MetaHandler) ListTags(c *gin.Context) {
	limit, err := cursor.ParseLimit(c.Query("limit"), cursor.DefaultLimit, cursor.MaxLimit)
	if err != nil {
		response.Fail(c, err)
		return
	}

	res, err := h.wallpaperSvc.ListTags(c.Request.Context(), c.Query("q"), limit)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}
