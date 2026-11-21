// Package handler 是 HTTP 传输层：路由、参数绑定与统一响应。
package handler

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
	"wallpaper-feed/internal/storage"
)

type PostHandler struct {
	svc *service.PostService
}

func NewPostHandler(svc *service.PostService) *PostHandler { return &PostHandler{svc: svc} }

// CreateUploadToken 对应 OpenAPI 路由 POST /api/v1/upload-tokens，支持 scene=post|avatar，返回 201。
func (h *PostHandler) CreateUploadToken(c *gin.Context) {
	scene := storage.UploadScenePost
	if c.Request.URL.Query().Has("scene") {
		raw := c.Query("scene")
		if raw == "" {
			response.Error(c, errno.ErrParam.WithMsg("不支持的上传场景"))
			return
		}
		scene = storage.UploadScene(raw)
	}
	tok, err := h.svc.UploadToken(c.Request.Context(), scene, middleware.UserID(c))
	if err != nil {
		response.Error(c, err)
		return
	}
	response.Created(c, tok)
}

// UploadToken 是模块 01 兼容路由 GET /api/v1/upload/token，固定用于帖子上传并返回 200。
func (h *PostHandler) UploadToken(c *gin.Context) {
	tok, err := h.svc.UploadToken(c.Request.Context(), storage.UploadScenePost, middleware.UserID(c))
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, tok)
}

func (h *PostHandler) Create(c *gin.Context) {
	var req service.CreatePostReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errno.ErrParam)
		return
	}
	dto, err := h.svc.Create(c.Request.Context(), middleware.UserID(c), &req)
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, dto)
}

func (h *PostHandler) Get(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		response.Error(c, errno.ErrParam)
		return
	}
	dto, err := h.svc.Get(c.Request.Context(), id)
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, dto)
}
