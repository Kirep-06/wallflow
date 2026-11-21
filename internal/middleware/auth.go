// Package middleware 提供 HTTP 中间件。
package middleware

import (
	"context"
	"strings"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
)

const (
	userIDKey    = "user_id"
	sessionIDKey = "session_id"
)

// TokenAuthenticator 校验 Bearer Access Token 并检查对应会话。
type TokenAuthenticator interface {
	Authenticate(ctx context.Context, rawToken string) (userID uint64, sid string, err error)
}

// AuthRequired 强制鉴权中间件：缺少、格式非法或无效凭证返回 10003；Redis 故障返回 10001。
func AuthRequired(auth TokenAuthenticator) gin.HandlerFunc {
	return func(c *gin.Context) {
		tok, present, valid := extractBearerToken(c)
		if !present || !valid {
			response.Error(c, errno.ErrUnauth)
			return
		}
		uid, sid, err := auth.Authenticate(c.Request.Context(), tok)
		if err != nil {
			response.Error(c, err)
			return
		}
		c.Set(userIDKey, uid)
		c.Set(sessionIDKey, sid)
		c.Next()
	}
}

// AuthOptional 可选鉴权中间件：完全不带 Authorization 头时匿名放行；携带空值、格式错误或无效凭证返回 10003。
func AuthOptional(auth TokenAuthenticator) gin.HandlerFunc {
	return func(c *gin.Context) {
		tok, present, valid := extractBearerToken(c)
		if !present {
			c.Next()
			return
		}
		if !valid {
			response.Error(c, errno.ErrUnauth)
			return
		}
		uid, sid, err := auth.Authenticate(c.Request.Context(), tok)
		if err != nil {
			response.Error(c, err)
			return
		}
		c.Set(userIDKey, uid)
		c.Set(sessionIDKey, sid)
		c.Next()
	}
}

// UserID 返回当前鉴权通过的用户 ID；匿名请求返回 0。
func UserID(c *gin.Context) uint64 {
	v, _ := c.Get(userIDKey)
	uid, _ := v.(uint64)
	return uid
}

// SessionID 返回当前鉴权通过的会话 ID；匿名请求返回空字符串。
func SessionID(c *gin.Context) string {
	v, _ := c.Get(sessionIDKey)
	sid, _ := v.(string)
	return sid
}

func extractBearerToken(c *gin.Context) (token string, present bool, valid bool) {
	vals := c.Request.Header.Values("Authorization")
	if len(vals) == 0 {
		return "", false, false
	}
	if len(vals) > 1 {
		return "", true, false
	}
	raw := vals[0]
	if raw == "" {
		return "", true, false
	}
	scheme, rest, found := strings.Cut(raw, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || rest == "" || strings.ContainsAny(rest, " \t\r\n") {
		return "", true, false
	}
	return rest, true, true
}
