package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/response"
)

// NewRouter 装配 HTTP 路由。
func NewRouter(isDev bool, post *PostHandler, feed *FeedHandler) *gin.Engine {
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
		v1.GET("/posts/:id", post.Get)
		v1.GET("/feed/latest", feed.Latest)

		authed := v1.Group("", middleware.DevAuth(isDev))
		authed.GET("/upload/token", post.UploadToken)
		authed.POST("/posts", post.Create)
	}
	return r
}
