package handler

import (
	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

// UploadHandler 处理 M2 客户端直传凭证签发接口。
type UploadHandler struct {
	uploadSvc *service.UploadService
}

// NewUploadHandler 构造上传处理器。
func NewUploadHandler(uploadSvc *service.UploadService) *UploadHandler {
	return &UploadHandler{uploadSvc: uploadSvc}
}

// CreateUploadToken 处理 POST /api/v1/uploads/token。
func (h *UploadHandler) CreateUploadToken(c *gin.Context) {
	var req model.UploadTokenRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		response.Fail(c, err)
		return
	}

	uid := middleware.GetUserID(c)
	res, err := h.uploadSvc.CreateUploadToken(c.Request.Context(), uid, req)
	if err != nil {
		response.Fail(c, err)
		return
	}

	c.Header("Cache-Control", "no-store")
	response.OK(c, res)
}
