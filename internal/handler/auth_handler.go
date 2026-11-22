package handler

import (
	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

// AuthHandler 处理 M1 认证相关 HTTP 接口。
type AuthHandler struct {
	authSvc *service.AuthService
}

// NewAuthHandler 构造认证处理器。
func NewAuthHandler(authSvc *service.AuthService) *AuthHandler {
	return &AuthHandler{authSvc: authSvc}
}

// Register 处理 POST /api/v1/auth/register。
func (h *AuthHandler) Register(c *gin.Context) {
	var req model.RegisterRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		response.Fail(c, err)
		return
	}

	res, err := h.authSvc.Register(c.Request.Context(), req)
	if err != nil {
		response.Fail(c, err)
		return
	}

	c.Header("Cache-Control", "no-store")
	response.Created(c, res)
}

// Login 处理 POST /api/v1/auth/login。
func (h *AuthHandler) Login(c *gin.Context) {
	var req model.LoginRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		response.Fail(c, err)
		return
	}

	res, err := h.authSvc.Login(c.Request.Context(), req)
	if err != nil {
		response.Fail(c, err)
		return
	}

	c.Header("Cache-Control", "no-store")
	response.OK(c, res)
}

// Refresh 处理 POST /api/v1/auth/refresh。
func (h *AuthHandler) Refresh(c *gin.Context) {
	var req model.RefreshRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		response.Fail(c, err)
		return
	}

	res, err := h.authSvc.Refresh(c.Request.Context(), req)
	if err != nil {
		response.Fail(c, err)
		return
	}

	c.Header("Cache-Control", "no-store")
	response.OK(c, res)
}

// Logout 处理 POST /api/v1/auth/logout。
func (h *AuthHandler) Logout(c *gin.Context) {
	sid := middleware.GetSessionID(c)
	if err := h.authSvc.Logout(c.Request.Context(), sid); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, nil)
}
