package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/response"
)

// NewRouter 装配 HTTP 路由。
func NewRouter(
	isDev bool,
	authenticator middleware.TokenAuthenticator,
	auth *AuthHandler,
	user *UserHandler,
	follow *FollowHandler,
	post *PostHandler,
	feed *FeedHandler,
) *gin.Engine {
	if !isDev {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, response.Body{Code: 0, Msg: "ok"})
	})

	v1 := r.Group("/api/v1")
	{
		// 公开路由
		v1.POST("/auth/register", auth.Register)
		v1.POST("/auth/login", auth.Login)
		v1.POST("/auth/refresh", auth.Refresh)
		v1.GET("/posts/:id", post.Get)
		v1.GET("/feed/latest", feed.Latest)
		v1.GET("/users/:id/posts", user.ListPosts)

		// 可选鉴权路由
		optionalAuth := v1.Group("", middleware.AuthOptional(authenticator))
		optionalAuth.GET("/users/:id", user.GetByID)
		optionalAuth.GET("/users/:id/following", follow.ListFollowing)
		optionalAuth.GET("/users/:id/followers", follow.ListFollowers)

		// 强制鉴权路由
		authed := v1.Group("", middleware.AuthRequired(authenticator))
		authed.POST("/auth/logout", auth.Logout)
		authed.GET("/users/me", user.GetMe)
		authed.PATCH("/users/me", user.PatchMe)
		authed.PUT("/users/:id/follow", follow.Follow)
		authed.DELETE("/users/:id/follow", follow.Unfollow)
		authed.POST("/upload-tokens", post.CreateUploadToken)
		authed.GET("/upload/token", post.UploadToken) // 模块 01 兼容路由（固定帖子上传，200）
		authed.POST("/posts", post.Create)
	}
	return r
}
