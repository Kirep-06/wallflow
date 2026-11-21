package handler

import (
	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

type AuthHandler struct {
	svc *service.AuthService
}

func NewAuthHandler(svc *service.AuthService) *AuthHandler { return &AuthHandler{svc: svc} }

func (h *AuthHandler) Register(c *gin.Context) {
	var req service.RegisterReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errno.ErrParam)
		return
	}
	data, err := h.svc.Register(c.Request.Context(), &req)
	if err != nil {
		response.Error(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	response.Created(c, data)
}

func (h *AuthHandler) Login(c *gin.Context) {
	var req service.LoginReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errno.ErrParam)
		return
	}
	data, err := h.svc.Login(c.Request.Context(), &req)
	if err != nil {
		response.Error(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	response.OK(c, data)
}

func (h *AuthHandler) Refresh(c *gin.Context) {
	var req service.RefreshReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errno.ErrParam)
		return
	}
	data, err := h.svc.Refresh(c.Request.Context(), &req)
	if err != nil {
		response.Error(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	response.OK(c, data)
}

func (h *AuthHandler) Logout(c *gin.Context) {
	if err := h.svc.Logout(c.Request.Context(), middleware.SessionID(c)); err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, nil)
}
