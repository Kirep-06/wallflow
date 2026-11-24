package handler

import (
	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/middleware"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/response"
	"wallpaper-feed/internal/service"
)

// RouterDeps 聚合路由注册所需的最小依赖。
type RouterDeps struct {
	IsDev              bool
	AuthService        *service.AuthService
	AuthHandler        *AuthHandler
	UserHandler        *UserHandler
	FollowHandler      *FollowHandler
	FeedHandler        *FeedHandler
	MetaHandler        *MetaHandler
	UploadHandler      *UploadHandler
	WallpaperHandler   *WallpaperHandler
	InteractionHandler *InteractionHandler
	AdminMetaService   *service.AdminMetaService
	AdminMetaHandler   *AdminMetaHandler
}

// NewRouter 装配全局中间件与全部 HTTP 路由（M1 ~ M7）。
func NewRouter(deps RouterDeps) *gin.Engine {
	if deps.IsDev {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(middleware.Recovery(), middleware.RequestLog())
	if deps.IsDev {
		r.Use(middleware.CORS(true, nil))
	}

	r.NoRoute(func(c *gin.Context) {
		response.Fail(c, errno.ErrNotFound)
	})

	// 存活探测
	r.GET("/healthz", func(c *gin.Context) {
		response.OK(c, gin.H{"status": "ok"})
	})

	authRequired := middleware.AuthRequired(deps.AuthService)
	authOptional := middleware.AuthOptional(deps.AuthService)

	v1 := r.Group("/api/v1")
	{
		// M1: 认证与用户
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
			users.GET("/me/favorites", authRequired, deps.InteractionHandler.ListMyFavorites)
			users.GET("/me/likes", authRequired, deps.InteractionHandler.ListMyLikes)
			users.GET("/:id", authOptional, deps.UserHandler.GetPublicProfile)
			users.GET("/:id/wallpapers", authOptional, deps.WallpaperHandler.ListUserWallpapers)
			users.POST("/:id/follow", authRequired, deps.FollowHandler.Follow)
			users.DELETE("/:id/follow", authRequired, deps.FollowHandler.Unfollow)
			users.GET("/:id/following", authOptional, deps.FollowHandler.ListFollowing)
			users.GET("/:id/followers", authOptional, deps.FollowHandler.ListFollowers)
		}

		// M2 + M3 + M6: 分类、标签、直传凭证、壁纸画廊与互动
		v1.GET("/categories", deps.MetaHandler.ListCategories)
		v1.GET("/tags", deps.MetaHandler.ListTags)
		v1.POST("/uploads/token", authRequired, deps.UploadHandler.CreateUploadToken)

		wallpapers := v1.Group("/wallpapers")
		{
			wallpapers.GET("", authOptional, deps.WallpaperHandler.ListWallpapers)
			wallpapers.GET("/:id", authOptional, deps.WallpaperHandler.GetWallpaperDetail)
			wallpapers.POST("", authRequired, deps.WallpaperHandler.CreateWallpaper)
			wallpapers.DELETE("/:id", authRequired, deps.WallpaperHandler.DeleteWallpaper)
			wallpapers.POST("/:id/like", authRequired, deps.InteractionHandler.Like)
			wallpapers.DELETE("/:id/like", authRequired, deps.InteractionHandler.Unlike)
			wallpapers.POST("/:id/favorite", authRequired, deps.InteractionHandler.Favorite)
			wallpapers.DELETE("/:id/favorite", authRequired, deps.InteractionHandler.Unfavorite)
			wallpapers.POST("/:id/download", authOptional, deps.InteractionHandler.RecordDownload)
			wallpapers.GET("/:id/comments", authOptional, deps.InteractionHandler.ListComments)
			wallpapers.POST("/:id/comments", authRequired, deps.InteractionHandler.CreateComment)
		}

		// M6: 一级评论管理
		v1.DELETE("/comments/:id", authRequired, deps.InteractionHandler.DeleteComment)

		// M5 / M7: 推拉结合关注流与全站热门榜
		v1.GET("/feed/following", authRequired, deps.FeedHandler.ListFollowingFeed)
		v1.GET("/feed/hot", authOptional, deps.FeedHandler.ListHotFeed)

		// 管理后台：分类与话题（标签）管理（白名单管理员鉴权）
		if deps.AdminMetaHandler != nil {
			admin := v1.Group("/admin", authRequired, middleware.AdminRequired(deps.AdminMetaService))
			{
				admin.GET("/me", deps.AdminMetaHandler.GetMe)
				admin.GET("/categories", deps.AdminMetaHandler.ListCategories)
				admin.POST("/categories", deps.AdminMetaHandler.CreateCategory)
				admin.PATCH("/categories/:id", deps.AdminMetaHandler.UpdateCategory)
				admin.DELETE("/categories/:id", deps.AdminMetaHandler.DeleteCategory)
				admin.GET("/tags", deps.AdminMetaHandler.ListTags)
				admin.POST("/tags", deps.AdminMetaHandler.CreateTag)
				admin.PATCH("/tags/:id", deps.AdminMetaHandler.UpdateTag)
				admin.DELETE("/tags/:id", deps.AdminMetaHandler.DeleteTag)
			}
		}
	}

	return r
}
