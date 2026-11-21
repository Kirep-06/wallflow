package handler

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

type UserHandler struct {
	svc *service.UserService
}

func NewUserHandler(svc *service.UserService) *UserHandler { return &UserHandler{svc: svc} }

func (h *UserHandler) GetMe(c *gin.Context) {
	uid := middleware.UserID(c)
	profile, err := h.svc.GetProfile(c.Request.Context(), uid, uid)
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, profile)
}

func (h *UserHandler) PatchMe(c *gin.Context) {
	var req service.PatchProfileReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, errno.ErrParam)
		return
	}
	profile, err := h.svc.UpdateProfile(c.Request.Context(), middleware.UserID(c), &req)
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, profile)
}

func (h *UserHandler) GetByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		response.Error(c, errno.ErrParam)
		return
	}
	profile, err := h.svc.GetProfile(c.Request.Context(), id, middleware.UserID(c))
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, profile)
}

func (h *UserHandler) ListPosts(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		response.Error(c, errno.ErrParam)
		return
	}
	var limit int
	if c.Request.URL.Query().Has("limit") {
		raw := c.Query("limit")
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 || v > service.UserPostsPageMax {
			response.Error(c, errno.ErrParam)
			return
		}
		limit = v
	}
	page, err := h.svc.ListUserPosts(c.Request.Context(), id, c.Query("cursor"), limit)
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, page)
}
