package handler

import (
	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

// WallpaperHandler 处理 M2 壁纸投稿发布与作者软删接口。
type WallpaperHandler struct {
	wallpaperSvc *service.WallpaperService
}

// NewWallpaperHandler 构造壁纸处理器。
func NewWallpaperHandler(wallpaperSvc *service.WallpaperService) *WallpaperHandler {
	return &WallpaperHandler{wallpaperSvc: wallpaperSvc}
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
