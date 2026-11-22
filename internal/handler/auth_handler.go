package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

const maxJSONBodyBytes = 1 << 20 // 1 MiB

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

// decodeStrictJSON 严格解析 JSON 请求体：限制大小、拒绝空体/顶层 null/未知字段与尾随脏数据。
func decodeStrictJSON(c *gin.Context, dst any) error {
	if c.Request.Body == nil {
		return errno.ErrInvalidParam
	}
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, maxJSONBodyBytes+1))
	if err != nil || len(raw) > maxJSONBodyBytes {
		return errno.ErrInvalidParam
	}

	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return errno.ErrInvalidParam
	}

	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errno.ErrInvalidParam
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errno.ErrInvalidParam
	}
	return nil
}
