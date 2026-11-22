package handler

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"slices"
	"time"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
)

// RouterDeps 聚合路由注册所需的依赖。
type RouterDeps struct {
	Config        *config.Config
	Logger        *slog.Logger
	Authenticator middleware.Authenticator
	AuthHandler   *AuthHandler
	UserHandler   *UserHandler
}

// NewRouter 装配全局中间件与 M1 全部 8 个 HTTP 路由。
func NewRouter(deps RouterDeps) *gin.Engine {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}

	isDev := deps.Config != nil && deps.Config.IsDev()
	if isDev {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.HandleMethodNotAllowed = true

	r.Use(recoveryMiddleware(logger))
	r.Use(requestLogMiddleware(logger))
	if deps.Config != nil && (isDev || len(deps.Config.Server.CORSAllowOrigins) > 0) {
		r.Use(corsMiddleware(isDev, deps.Config.Server.CORSAllowOrigins))
	}

	r.NoRoute(func(c *gin.Context) {
		response.Fail(c, errno.ErrNotFound)
	})
	r.NoMethod(func(c *gin.Context) {
		response.Fail(c, errno.ErrNotFound)
	})

	// 1. 存活探测
	r.GET("/healthz", func(c *gin.Context) {
		response.OK(c, gin.H{"status": "ok"})
	})

	// 2~8. M1 认证与用户路由
	v1 := r.Group("/api/v1")
	{
		auth := v1.Group("/auth")
		{
			auth.POST("/register", deps.AuthHandler.Register)
			auth.POST("/login", deps.AuthHandler.Login)
			auth.POST("/refresh", deps.AuthHandler.Refresh)
			auth.POST("/logout", middleware.AuthRequired(deps.Authenticator), deps.AuthHandler.Logout)
		}

		users := v1.Group("/users")
		{
			users.GET("/me", middleware.AuthRequired(deps.Authenticator), deps.UserHandler.GetMe)
			users.PATCH("/me", middleware.AuthRequired(deps.Authenticator), deps.UserHandler.PatchMe)
			users.GET("/:id", middleware.AuthOptional(deps.Authenticator), deps.UserHandler.GetPublicProfile)
		}
	}

	return r
}

func recoveryMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				logger.ErrorContext(c.Request.Context(), "http handler panic recovered",
					slog.String("method", c.Request.Method),
					slog.String("path", c.Request.URL.Path),
					slog.Any("panic", rec),
					slog.String("stack", string(debug.Stack())),
				)
				response.AbortWithError(c, errno.ErrInternal)
			}
		}()
		c.Next()
	}
}

func requestLogMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		status := c.Writer.Status()
		attrs := []any{
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.Int("status", status),
			slog.Int64("latency_ms", time.Since(start).Milliseconds()),
			slog.String("client_ip", c.ClientIP()),
		}

		switch {
		case status >= http.StatusInternalServerError:
			logger.ErrorContext(c.Request.Context(), "http request", attrs...)
		case status >= http.StatusBadRequest:
			logger.WarnContext(c.Request.Context(), "http request", attrs...)
		default:
			logger.InfoContext(c.Request.Context(), "http request", attrs...)
		}
	}
}

func corsMiddleware(isDev bool, allowOrigins []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin == "" {
			c.Next()
			return
		}

		allowed := slices.Contains(allowOrigins, "*") || slices.Contains(allowOrigins, origin)
		if !allowed && isDev && len(allowOrigins) == 0 {
			allowed = true
		}

		if allowed {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type")
			c.Header("Access-Control-Max-Age", "86400")
		}

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
