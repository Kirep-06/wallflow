// Package handler 是 HTTP 传输层：路由、参数绑定与统一响应。
package handler

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

type PostHandler struct {
	svc *service.PostService
}

func NewPostHandler(svc *service.PostService) *PostHandler { return &PostHandler{svc: svc} }

func (h *PostHandler) UploadToken(c *gin.Context) {
	tok, err := h.svc.UploadToken(c.Request.Context())
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
