package model

import "time"

const (
	WallpaperStatusActive  uint8 = 1
	WallpaperStatusDeleted uint8 = 2

	OrientationLandscape uint8 = 1
	OrientationPortrait  uint8 = 2
	OrientationSquare    uint8 = 3
)

// Tag 对应 MySQL tags 表实体。
type Tag struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement"`
	Name      string    `gorm:"type:varchar(24);not null;uniqueIndex:uk_name"`
	UseCount  uint64    `gorm:"not null;default:0"`
	IconKey   string    `gorm:"type:varchar(255);not null;default:''"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// TableName 指定 GORM 表名。
func (Tag) TableName() string {
	return "tags"
}

// ToItem 将标签实体转换为接口响应项（iconURL 由服务层拼装后传入）。
func (t *Tag) ToItem(iconURL ...string) TagItem {
	url := ""
	if len(iconURL) > 0 {
		url = iconURL[0]
	}
	return TagItem{
		ID:       t.ID,
		Name:     t.Name,
		UseCount: t.UseCount,
		IconURL:  url,
	}
}

// Wallpaper 对应 MySQL wallpapers 静态主表实体（高频互动计数由 M6 wallpaper_stats 独立承载）。
type Wallpaper struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement"`
	UserID      uint64    `gorm:"not null"`
	Title       string    `gorm:"type:varchar(80);not null"`
	Description string    `gorm:"type:varchar(500);not null;default:''"`
	ObjectKey   string    `gorm:"type:varchar(255);not null;uniqueIndex:uk_object_key"`
	Width       uint32    `gorm:"not null"`
	Height      uint32    `gorm:"not null"`
	Orientation uint8     `gorm:"not null"`
	SizeBytes   uint64    `gorm:"not null"`
	Status      uint8     `gorm:"not null;default:1"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

// TableName 指定 GORM 表名。
func (Wallpaper) TableName() string {
	return "wallpapers"
}

// WallpaperTag 对应 MySQL wallpaper_tags 关联表实体。
type WallpaperTag struct {
	WallpaperID uint64 `gorm:"primaryKey"`
	TagID       uint64 `gorm:"primaryKey"`
}

// TableName 指定 GORM 表名。
func (WallpaperTag) TableName() string {
	return "wallpaper_tags"
}

// WallpaperImage 对应 MySQL wallpaper_images 一帖多图子表实体。
type WallpaperImage struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement"`
	WallpaperID uint64    `gorm:"not null"`
	SortNo      uint32    `gorm:"not null;default:0"`
	ObjectKey   string    `gorm:"type:varchar(255);not null;uniqueIndex:uk_object_key"`
	Width       uint32    `gorm:"not null"`
	Height      uint32    `gorm:"not null"`
	Orientation uint8     `gorm:"not null"`
	SizeBytes   uint64    `gorm:"not null"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

// TableName 指定 GORM 表名。
func (WallpaperImage) TableName() string {
	return "wallpaper_images"
}

// WallpaperImageCache 为三级缓存中单张图片的精简快照结构（仅存相对 object_key）。
type WallpaperImageCache struct {
	ID          uint64 `json:"id,string"`
	SortNo      uint32 `json:"sort_no"`
	ObjectKey   string `json:"object_key"`
	Width       uint32 `json:"width"`
	Height      uint32 `json:"height"`
	Orientation uint8  `json:"orientation"`
	SizeBytes   uint64 `json:"size_bytes"`
}

// WallpaperImageItem 为接口响应中的单张图片结构。
type WallpaperImageItem struct {
	ID          uint64 `json:"id,string"`
	SortNo      uint32 `json:"sort_no"`
	ThumbURL    string `json:"thumb_url"`
	RawURL      string `json:"raw_url"`
	Width       uint32 `json:"width"`
	Height      uint32 `json:"height"`
	Orientation uint8  `json:"orientation"`
	SizeBytes   uint64 `json:"size_bytes"`
	Mime        string `json:"mime"`
}

// WallpaperOwnerRecord 为软删鉴权查询的轻量投影结构。
type WallpaperOwnerRecord struct {
	ID     uint64
	UserID uint64
	Status uint8
}

// WallpaperDetailCache 为 wallpaper:detail:{id} 三级缓存快照结构。
// 仅保存相对 object_key 与不可变静态元数据（含绑定标签与图片列表），
// 严格隔离可变作者资料（user:profile:{uid}）与高频互动计数（M6 wallpaper:stats:{id}）。
type WallpaperDetailCache struct {
	ID          uint64                `json:"id,string"`
	UserID      uint64                `json:"user_id,string"`
	Title       string                `json:"title"`
	Description string                `json:"description"`
	ObjectKey   string                `json:"object_key"`
	Width       uint32                `json:"width"`
	Height      uint32                `json:"height"`
	Orientation uint8                 `json:"orientation"`
	SizeBytes   uint64                `json:"size_bytes"`
	Tags        []TagItem             `json:"tags"`
	Images      []WallpaperImageCache `json:"images,omitempty"`
	CreatedAt   time.Time             `json:"created_at"`
}

// WallpaperListQuery 为发现页最新流与单标签筛选查询参数。
type WallpaperListQuery struct {
	Cursor string
	Limit  int
	TagID  uint64
}

// CalcOrientation 按 ±5% 容差计算图片方向（1-横图 2-竖图 3-方图）。
func CalcOrientation(width, height uint32) uint8 {
	if width == 0 || height == 0 {
		return OrientationSquare
	}
	ratio := float64(width) / float64(height)
	if ratio > 1.05 {
		return OrientationLandscape
	}
	if ratio < 0.95 {
		return OrientationPortrait
	}
	return OrientationSquare
}

// TagItem 为标签响应项。
type TagItem struct {
	ID       uint64 `json:"id,string"`
	Name     string `json:"name"`
	UseCount uint64 `json:"use_count"`
	IconURL  string `json:"icon_url,omitempty"`
}

// TagList 为热门或联想标签列表响应结构。
type TagList struct {
	Items []TagItem `json:"items"`
}

// AdminCreateTagRequest 为后台新建标签请求体。
type AdminCreateTagRequest struct {
	Name    string `json:"name"`
	IconKey string `json:"icon_key"`
}

// AdminUpdateTagRequest 为后台编辑标签请求体（指针区分未传与空值）。
type AdminUpdateTagRequest struct {
	Name    *string `json:"name"`
	IconKey *string `json:"icon_key"`
}

// UploadTokenRequest 为获取直传凭证请求体。
type UploadTokenRequest struct {
	Scene       string `json:"scene"`
	ContentType string `json:"content_type"`
	FileSize    int64  `json:"file_size"`
}

// UploadTokenResult 为直传凭证响应结构。
type UploadTokenResult struct {
	UploadToken  string `json:"upload_token"`
	UploadURL    string `json:"upload_url"`
	ObjectKey    string `json:"object_key"`
	ExpiresIn    int64  `json:"expires_in"`
	MaxSizeBytes int64  `json:"max_size_bytes"`
}

// CreateWallpaperRequest 为投稿发布请求体（支持 object_keys 1-9 张多图，兼容旧 object_key 单图）。
type CreateWallpaperRequest struct {
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	ObjectKey   string   `json:"object_key"`
	ObjectKeys  []string `json:"object_keys"`
}

// WallpaperAuthor 为详情与列表内嵌作者信息。
type WallpaperAuthor struct {
	ID          uint64 `json:"id,string"`
	Username    string `json:"username"`
	Nickname    string `json:"nickname"`
	AvatarURL   string `json:"avatar_url"`
	IsFollowing bool   `json:"is_following"`
}

// WallpaperDetail 为作品详情响应结构。
type WallpaperDetail struct {
	ID            uint64               `json:"id,string"`
	Title         string               `json:"title"`
	Description   string               `json:"description"`
	ThumbURL      string               `json:"thumb_url"`
	RawURL        string               `json:"raw_url"`
	Width         uint32               `json:"width"`
	Height        uint32               `json:"height"`
	Orientation   uint8                `json:"orientation"`
	SizeBytes     uint64               `json:"size_bytes"`
	Mime          string               `json:"mime"`
	LikeCount     uint64               `json:"like_count"`
	FavCount      uint64               `json:"fav_count"`
	DownloadCount uint64               `json:"download_count"`
	CommentCount  uint64               `json:"comment_count"`
	IsLiked       bool                 `json:"is_liked"`
	IsFavorited   bool                 `json:"is_favorited"`
	Tags          []TagItem            `json:"tags"`
	Images        []WallpaperImageItem `json:"images"`
	Author        WallpaperAuthor      `json:"author"`
	CreatedAt     time.Time            `json:"created_at"`
}

// WallpaperSummary 为帖子卡片摘要响应结构。
type WallpaperSummary struct {
	ID            uint64               `json:"id,string"`
	Title         string               `json:"title"`
	Description   string               `json:"description,omitempty"`
	ThumbURL      string               `json:"thumb_url"`
	Width         uint32               `json:"width"`
	Height        uint32               `json:"height"`
	Orientation   uint8                `json:"orientation"`
	HotScore      float64              `json:"hot_score,omitempty"`
	LikeCount     uint64               `json:"like_count"`
	FavCount      uint64               `json:"fav_count"`
	DownloadCount uint64               `json:"download_count"`
	CommentCount  uint64               `json:"comment_count"`
	IsLiked       bool                 `json:"is_liked"`
	IsFavorited   bool                 `json:"is_favorited"`
	Tags          []TagItem            `json:"tags"`
	Images        []WallpaperImageItem `json:"images"`
	Author        WallpaperAuthor      `json:"author"`
	CreatedAt     time.Time            `json:"created_at"`
}
