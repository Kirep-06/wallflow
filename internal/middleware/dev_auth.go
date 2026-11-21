// Package middleware 提供 HTTP 中间件。
package middleware

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
)

const userIDKey = "user_id"

// DevAuth 是模块 02（注册/登录）完成前的临时鉴权：仅在 dev 环境从 X-User-ID 读取用户 ID。
func DevAuth(isDev bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !isDev {
			response.Error(c, errno.ErrUnauth)
			return
		}
		raw := c.GetHeader("X-User-ID")
		uid, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || uid == 0 {
			response.Error(c, errno.ErrUnauth)
			return
		}
		c.Set(userIDKey, uid)
		c.Next()
	}
}

func UserID(c *gin.Context) uint64 {
	v, _ := c.Get(userIDKey)
	uid, _ := v.(uint64)
	return uid
}
