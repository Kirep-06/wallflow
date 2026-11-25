package handler

import (
	"errors"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
	"wallpaper-feed/internal/storage"
)

const localUploadBodyOverhead int64 = 1 << 20 // 预留 1 MiB 给 multipart 表单边界与字段

// UploadHandler 处理客户端直传凭证签发及本地存储上传/读取接口。
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

// UploadLocalFile 处理 POST /api/v1/uploads/local（仅 storage.provider=local 时启用）。
func (h *UploadHandler) UploadLocalFile(c *gin.Context) {
	ls := h.uploadSvc.LocalStorage()
	if ls == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "local storage is not enabled"})
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, storage.WallpaperMaxBytes+localUploadBodyOverhead)
	token := c.PostForm("token")
	key := c.PostForm("key")
	fh, err := c.FormFile("file")
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "file exceeds size limit"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing file"})
		return
	}

	f, err := fh.Open()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read uploaded file"})
		return
	}
	defer f.Close()

	if err := ls.SaveUpload(c.Request.Context(), token, key, f); err != nil {
		switch {
		case errors.Is(err, storage.ErrInvalidUploadToken):
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired upload token"})
		case errors.Is(err, storage.ErrUploadTooLarge):
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "file exceeds size limit"})
		case errors.Is(err, storage.ErrInvalidImage):
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid image file"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "save upload failed"})
		}
		return
	}

	c.JSON(http.StatusOK, gin.H{"key": key})
}

// ServeLocalFile 处理 GET /api/v1/uploads/files/*filepath（仅 storage.provider=local 时启用）。
func (h *UploadHandler) ServeLocalFile(c *gin.Context) {
	ls := h.uploadSvc.LocalStorage()
	if ls == nil {
		c.Status(http.StatusNotFound)
		return
	}

	fullPath, err := ls.ResolveFilePath(c.Param("filepath"))
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}
	stat, err := os.Stat(fullPath)
	if err != nil || stat.IsDir() {
		c.Status(http.StatusNotFound)
		return
	}

	c.Header("Cache-Control", "public, max-age=31536000, immutable")
	c.Header("X-Content-Type-Options", "nosniff")
	c.File(fullPath)
}
