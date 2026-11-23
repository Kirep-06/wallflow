package handler

import (
	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

// WallpaperHandler 处理壁纸投稿软删（M2）以及画廊列表、详情与创作者作品流（M3）接口。
type WallpaperHandler struct {
	wallpaperSvc *service.WallpaperService
}

// NewWallpaperHandler 构造壁纸处理器。
func NewWallpaperHandler(wallpaperSvc *service.WallpaperService) *WallpaperHandler {
	return &WallpaperHandler{wallpaperSvc: wallpaperSvc}
}

// ListWallpapers 处理 GET /api/v1/wallpapers（最新流与分类/标签单维互斥筛选）。
func (h *WallpaperHandler) ListWallpapers(c *gin.Context) {
	limit, err := cursor.ParseLimit(c.Query("limit"), cursor.DefaultLimit, cursor.MaxLimit)
	if err != nil {
		response.Fail(c, err)
		return
	}
	categoryID, err := parseOptionalPositiveUint64(c.Query("category_id"), "category_id 格式不合法")
	if err != nil {
		response.Fail(c, err)
		return
	}
	tagID, err := parseOptionalPositiveUint64(c.Query("tag_id"), "tag_id 格式不合法")
	if err != nil {
		response.Fail(c, err)
		return
	}

	res, err := h.wallpaperSvc.ListWallpapers(c.Request.Context(), model.WallpaperListQuery{
		Cursor:     c.Query("cursor"),
		Limit:      limit,
		CategoryID: categoryID,
		TagID:      tagID,
	}, middleware.GetUserID(c))
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

// GetWallpaperDetail 处理 GET /api/v1/wallpapers/:id。
func (h *WallpaperHandler) GetWallpaperDetail(c *gin.Context) {
	wallpaperID, err := parsePositiveUint64(c.Param("id"), "壁纸 ID 格式不合法")
	if err != nil {
		response.Fail(c, err)
		return
	}

	res, err := h.wallpaperSvc.GetWallpaperDetail(c.Request.Context(), wallpaperID, middleware.GetUserID(c))
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

// ListUserWallpapers 处理 GET /api/v1/users/:id/wallpapers。
func (h *WallpaperHandler) ListUserWallpapers(c *gin.Context) {
	targetUserID, err := parsePositiveUint64(c.Param("id"), "用户 ID 格式不合法")
	if err != nil {
		response.Fail(c, err)
		return
	}
	limit, err := cursor.ParseLimit(c.Query("limit"), cursor.DefaultLimit, cursor.MaxLimit)
	if err != nil {
		response.Fail(c, err)
		return
	}

	res, err := h.wallpaperSvc.ListUserWallpapers(c.Request.Context(), targetUserID, middleware.GetUserID(c), c.Query("cursor"), limit)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

// CreateWallpaper 处理 POST /api/v1/wallpapers。
func (h *WallpaperHandler) CreateWallpaper(c *gin.Context) {
	var req model.CreateWallpaperRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		response.Fail(c, err)
		return
	}

	uid := middleware.GetUserID(c)
	res, err := h.wallpaperSvc.CreateWallpaper(c.Request.Context(), uid, req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.Created(c, res)
}

// DeleteWallpaper 处理 DELETE /api/v1/wallpapers/:id。
func (h *WallpaperHandler) DeleteWallpaper(c *gin.Context) {
	wallpaperID, err := parsePositiveUint64(c.Param("id"), "壁纸 ID 格式不合法")
	if err != nil {
		response.Fail(c, err)
		return
	}

	uid := middleware.GetUserID(c)
	if err := h.wallpaperSvc.DeleteWallpaper(c.Request.Context(), uid, wallpaperID); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, nil)
}
