package middleware

import (
	"strings"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

const (
	ctxKeyUserID    = "wf_auth_user_id"
	ctxKeySessionID = "wf_auth_session_id"
)

// AuthRequired 强鉴权中间件：缺少或非法凭证返回 10003，Redis 故障 fail-closed 返回 10001。
func AuthRequired(authSvc *service.AuthService) gin.HandlerFunc {
	return func(c *gin.Context) {
		token, ok := extractSingleBearerHeader(c)
		if !ok {
			response.AbortWithError(c, errno.ErrUnauthorized)
			return
		}

		user, err := authSvc.Authenticate(c.Request.Context(), token)
		if err != nil {
			response.AbortWithError(c, err)
			return
		}

		c.Set(ctxKeyUserID, user.UserID)
		c.Set(ctxKeySessionID, user.SessionID)
		c.Next()
	}
}

// AuthOptional 可选鉴权中间件：未携带 Authorization 匿名放行（viewerID=0），
// 携带了但格式/签名/会话非法则返回 10003，Redis 故障返回 10001。
func AuthOptional(authSvc *service.AuthService) gin.HandlerFunc {
	return func(c *gin.Context) {
		headers := c.Request.Header.Values("Authorization")
		if len(headers) == 0 {
			c.Set(ctxKeyUserID, uint64(0))
			c.Next()
			return
		}
		if len(headers) > 1 {
			response.AbortWithError(c, errno.ErrUnauthorized)
			return
		}

		token, ok := parseBearerToken(headers[0])
		if !ok {
			response.AbortWithError(c, errno.ErrUnauthorized)
			return
		}

		user, err := authSvc.Authenticate(c.Request.Context(), token)
		if err != nil {
			response.AbortWithError(c, err)
			return
		}

		c.Set(ctxKeyUserID, user.UserID)
		c.Set(ctxKeySessionID, user.SessionID)
		c.Next()
	}
}

// GetUserID 从请求上下文读取当前用户 ID，匿名请求返回 0。
func GetUserID(c *gin.Context) uint64 {
	v, ok := c.Get(ctxKeyUserID)
	if !ok {
		return 0
	}
	uid, _ := v.(uint64)
	return uid
}

// GetSessionID 从请求上下文读取当前会话 ID。
func GetSessionID(c *gin.Context) string {
	v, ok := c.Get(ctxKeySessionID)
	if !ok {
		return ""
	}
	sid, _ := v.(string)
	return sid
}

func extractSingleBearerHeader(c *gin.Context) (string, bool) {
	headers := c.Request.Header.Values("Authorization")
	if len(headers) != 1 {
		return "", false
	}
	return parseBearerToken(headers[0])
}

func parseBearerToken(raw string) (string, bool) {
	scheme, token, found := strings.Cut(strings.TrimSpace(raw), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", false
	}
	return token, true
}
