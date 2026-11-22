package handler

import (
	"regexp"
	"strconv"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

var positiveIntIDPattern = regexp.MustCompile(`^[1-9][0-9]*$`)

// UserHandler 处理 M1 用户资料与创作者主页 HTTP 接口。
type UserHandler struct {
	userSvc *service.UserService
}

// NewUserHandler 构造用户处理器。
func NewUserHandler(userSvc *service.UserService) *UserHandler {
	return &UserHandler{userSvc: userSvc}
}

// GetMe 处理 GET /api/v1/users/me。
func (h *UserHandler) GetMe(c *gin.Context) {
	uid := middleware.GetUserID(c)
	profile, err := h.userSvc.GetMe(c.Request.Context(), uid)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, profile)
}

// PatchMe 处理 PATCH /api/v1/users/me。
func (h *UserHandler) PatchMe(c *gin.Context) {
	var req model.PatchProfileRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		response.Fail(c, err)
		return
	}

	uid := middleware.GetUserID(c)
	profile, err := h.userSvc.PatchMe(c.Request.Context(), uid, req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, profile)
}

// GetPublicProfile 处理 GET /api/v1/users/:id。
func (h *UserHandler) GetPublicProfile(c *gin.Context) {
	targetID, err := parsePositiveUint64(c.Param("id"))
	if err != nil {
		response.Fail(c, err)
		return
	}

	viewerID := middleware.GetUserID(c)
	profile, err := h.userSvc.GetPublicProfile(c.Request.Context(), targetID, viewerID)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, profile)
}

func parsePositiveUint64(raw string) (uint64, error) {
	if !positiveIntIDPattern.MatchString(raw) {
		return 0, errno.ErrInvalidParam.WithMessage("用户 ID 格式不合法")
	}
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		return 0, errno.ErrInvalidParam.WithMessage("用户 ID 格式不合法")
	}
	return id, nil
}
