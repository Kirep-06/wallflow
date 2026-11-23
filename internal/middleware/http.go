package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"slices"
	"time"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
)

// Recovery 捕获处理链中的 panic，记录堆栈并按统一 JSON 信封返回 500/10001。
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.ErrorContext(c.Request.Context(), "http handler panic recovered",
					slog.String("method", c.Request.Method),
					slog.String("path", c.Request.URL.Path),
					slog.Any("panic", rec),
					slog.String("stack", string(debug.Stack())),
				)
				if !c.Writer.Written() {
					response.AbortWithError(c, errno.ErrInternal)
					return
				}
				c.Abort()
			}
		}()
		c.Next()
	}
}

// RequestLog 记录基础 HTTP 访问日志（不打印 Authorization 与请求体），跳过 /healthz 正常探活。
func RequestLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		status := c.Writer.Status()
		if c.Request.URL.Path == "/healthz" && status < http.StatusBadRequest {
			return
		}

		attrs := []any{
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.Int("status", status),
			slog.Duration("latency", time.Since(start)),
			slog.String("client_ip", c.ClientIP()),
		}

		switch {
		case status >= http.StatusInternalServerError:
			slog.ErrorContext(c.Request.Context(), "http request", attrs...)
		case status >= http.StatusBadRequest:
			slog.WarnContext(c.Request.Context(), "http request", attrs...)
		default:
			slog.InfoContext(c.Request.Context(), "http request", attrs...)
		}
	}
}

// CORS 处理跨域响应头与 OPTIONS 预检请求。
func CORS(isDev bool, allowOrigins []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin == "" {
			c.Next()
			return
		}

		c.Writer.Header().Add("Vary", "Origin")
		allowed := slices.Contains(allowOrigins, "*") ||
			slices.Contains(allowOrigins, origin) ||
			(isDev && len(allowOrigins) == 0)

		if allowed {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type, If-None-Match")
			c.Header("Access-Control-Expose-Headers", "ETag")
			c.Header("Access-Control-Max-Age", "86400")
		}

		// 仅拦截真正的 CORS 预检请求。
		if c.Request.Method == http.MethodOptions && c.GetHeader("Access-Control-Request-Method") != "" {
			if allowed {
				c.AbortWithStatus(http.StatusNoContent)
			} else {
				c.AbortWithStatus(http.StatusForbidden)
			}
			return
		}
		c.Next()
	}
}
