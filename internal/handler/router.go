package handler

import (
	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

// RouterDeps 聚合路由注册所需的依赖。
type RouterDeps struct {
	Config      *config.Config
	AuthService *service.AuthService
	AuthHandler *AuthHandler
	UserHandler *UserHandler
}

// NewRouter 装配全局中间件与 M1 全部 8 个 HTTP 路由。
func NewRouter(deps RouterDeps) *gin.Engine {
	isDev := deps.Config != nil && deps.Config.IsDev()
	if isDev {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(middleware.Recovery(), middleware.RequestLog())
	if isDev {
		r.Use(middleware.CORS(true, nil))
	}

	r.NoRoute(func(c *gin.Context) {
		response.Fail(c, errno.ErrNotFound)
	})

	// 1. 存活探测
	r.GET("/healthz", func(c *gin.Context) {
		response.OK(c, gin.H{"status": "ok"})
	})

	authRequired := middleware.AuthRequired(deps.AuthService)
	authOptional := middleware.AuthOptional(deps.AuthService)

	// 2~8. M1 认证与用户路由
	v1 := r.Group("/api/v1")
	{
		auth := v1.Group("/auth")
		{
			auth.POST("/register", deps.AuthHandler.Register)
			auth.POST("/login", deps.AuthHandler.Login)
			auth.POST("/refresh", deps.AuthHandler.Refresh)
			auth.POST("/logout", authRequired, deps.AuthHandler.Logout)
		}

		users := v1.Group("/users")
		{
			users.GET("/me", authRequired, deps.UserHandler.GetMe)
			users.PATCH("/me", authRequired, deps.UserHandler.PatchMe)
			users.GET("/:id", authOptional, deps.UserHandler.GetPublicProfile)
		}
	}

	return r
}
