package handler

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

type FollowHandler struct {
	svc *service.FollowService
}

func NewFollowHandler(svc *service.FollowService) *FollowHandler {
	return &FollowHandler{svc: svc}
}

func (h *FollowHandler) Follow(c *gin.Context) {
	targetID, ok := parsePositiveIDParam(c, "id")
	if !ok {
		return
	}
	if err := h.svc.Follow(c.Request.Context(), middleware.UserID(c), targetID); err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, nil)
}

func (h *FollowHandler) Unfollow(c *gin.Context) {
	targetID, ok := parsePositiveIDParam(c, "id")
	if !ok {
		return
	}
	if err := h.svc.Unfollow(c.Request.Context(), middleware.UserID(c), targetID); err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, nil)
}

func (h *FollowHandler) ListFollowing(c *gin.Context) {
	targetID, ok := parsePositiveIDParam(c, "id")
	if !ok {
		return
	}
	limit, ok := parseFollowLimitQuery(c)
	if !ok {
		return
	}
	page, err := h.svc.ListFollowing(c.Request.Context(), targetID, middleware.UserID(c), c.Query("cursor"), limit)
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, page)
}

func (h *FollowHandler) ListFollowers(c *gin.Context) {
	targetID, ok := parsePositiveIDParam(c, "id")
	if !ok {
		return
	}
	limit, ok := parseFollowLimitQuery(c)
	if !ok {
		return
	}
	page, err := h.svc.ListFollowers(c.Request.Context(), targetID, middleware.UserID(c), c.Query("cursor"), limit)
	if err != nil {
		response.Error(c, err)
		return
	}
	response.OK(c, page)
}

func parsePositiveIDParam(c *gin.Context, name string) (uint64, bool) {
	id, err := strconv.ParseUint(c.Param(name), 10, 64)
	if err != nil || id == 0 {
		response.Error(c, errno.ErrParam)
		return 0, false
	}
	return id, true
}

func parseFollowLimitQuery(c *gin.Context) (int, bool) {
	if !c.Request.URL.Query().Has("limit") {
		return 0, true
	}
	v, err := strconv.Atoi(c.Query("limit"))
	if err != nil || v < 1 || v > service.FollowListPageMax {
		response.Error(c, errno.ErrParam)
		return 0, false
	}
	return v, true
}
