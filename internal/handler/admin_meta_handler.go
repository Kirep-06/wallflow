package handler

import (
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

// AdminMetaHandler 处理管理后台的管理员身份核验与分类/话题（标签）增删改查接口。
type AdminMetaHandler struct {
	svc *service.AdminMetaService
}

// NewAdminMetaHandler 构造后台元数据管理处理器。
func NewAdminMetaHandler(svc *service.AdminMetaService) *AdminMetaHandler {
	return &AdminMetaHandler{svc: svc}
}

// GetMe 处理 GET /api/v1/admin/me，校验并返回当前管理员基本信息。
func (h *AdminMetaHandler) GetMe(c *gin.Context) {
	user, err := h.svc.RequireAdmin(c.Request.Context(), middleware.GetUserID(c))
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, gin.H{
		"id":       strconv.FormatUint(user.ID, 10),
		"username": user.Username,
		"nickname": user.Nickname,
	})
}

// ListCategories 处理 GET /api/v1/admin/categories。
func (h *AdminMetaHandler) ListCategories(c *gin.Context) {
	res, err := h.svc.ListCategories(c.Request.Context())
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

// CreateCategory 处理 POST /api/v1/admin/categories。
func (h *AdminMetaHandler) CreateCategory(c *gin.Context) {
	var req model.AdminCreateCategoryRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		response.Fail(c, err)
		return
	}
	res, err := h.svc.CreateCategory(c.Request.Context(), middleware.GetUserID(c), req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

// UpdateCategory 处理 PATCH /api/v1/admin/categories/:id。
func (h *AdminMetaHandler) UpdateCategory(c *gin.Context) {
	id, err := parsePositiveUint64(c.Param("id"), "分类 ID 非法")
	if err != nil {
		response.Fail(c, err)
		return
	}
	var req model.AdminUpdateCategoryRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		response.Fail(c, err)
		return
	}
	res, err := h.svc.UpdateCategory(c.Request.Context(), middleware.GetUserID(c), id, req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

// DeleteCategory 处理 DELETE /api/v1/admin/categories/:id?migrate_to=<target_id>。
func (h *AdminMetaHandler) DeleteCategory(c *gin.Context) {
	id, err := parsePositiveUint64(c.Param("id"), "分类 ID 非法")
	if err != nil {
		response.Fail(c, err)
		return
	}
	var migrateToID uint64
	if raw := strings.TrimSpace(c.Query("migrate_to")); raw != "" {
		parsed, err := parsePositiveUint64(raw, "migrate_to 参数非法")
		if err != nil {
			response.Fail(c, err)
			return
		}
		migrateToID = parsed
	}
	if err := h.svc.DeleteCategory(c.Request.Context(), id, migrateToID); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, gin.H{"deleted": true})
}

// ListTags 处理 GET /api/v1/admin/tags。
func (h *AdminMetaHandler) ListTags(c *gin.Context) {
	limit := 200
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	res, err := h.svc.ListTags(c.Request.Context(), c.Query("q"), limit)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

// CreateTag 处理 POST /api/v1/admin/tags。
func (h *AdminMetaHandler) CreateTag(c *gin.Context) {
	var req model.AdminCreateTagRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		response.Fail(c, err)
		return
	}
	res, err := h.svc.CreateTag(c.Request.Context(), middleware.GetUserID(c), req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

// UpdateTag 处理 PATCH /api/v1/admin/tags/:id。
func (h *AdminMetaHandler) UpdateTag(c *gin.Context) {
	id, err := parsePositiveUint64(c.Param("id"), "话题 ID 非法")
	if err != nil {
		response.Fail(c, err)
		return
	}
	var req model.AdminUpdateTagRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		response.Fail(c, err)
		return
	}
	res, err := h.svc.UpdateTag(c.Request.Context(), middleware.GetUserID(c), id, req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, res)
}

// DeleteTag 处理 DELETE /api/v1/admin/tags/:id。
func (h *AdminMetaHandler) DeleteTag(c *gin.Context) {
	id, err := parsePositiveUint64(c.Param("id"), "话题 ID 非法")
	if err != nil {
		response.Fail(c, err)
		return
	}
	if err := h.svc.DeleteTag(c.Request.Context(), id); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, gin.H{"deleted": true})
}
