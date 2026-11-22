package model

import "time"

const (
	WallpaperStatusActive  uint8 = 1
	WallpaperStatusDeleted uint8 = 2

	OrientationLandscape uint8 = 1
	OrientationPortrait  uint8 = 2
	OrientationSquare    uint8 = 3
)

// Category 对应 MySQL categories 表实体。
type Category struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement"`
	Slug      string    `gorm:"type:varchar(32);not null;uniqueIndex:uk_slug"`
	Name      string    `gorm:"type:varchar(32);not null"`
	SortNo    int       `gorm:"not null;default:0"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// TableName 指定 GORM 表名。
func (Category) TableName() string {
	return "categories"
}

// ToItem 将分类实体转换为接口响应项。
func (c *Category) ToItem() CategoryItem {
	return CategoryItem{
		ID:     c.ID,
		Slug:   c.Slug,
		Name:   c.Name,
		SortNo: c.SortNo,
	}
}

// Tag 对应 MySQL tags 表实体。
type Tag struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement"`
	Name      string    `gorm:"type:varchar(24);not null;uniqueIndex:uk_name"`
	UseCount  uint64    `gorm:"not null;default:0"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// TableName 指定 GORM 表名。
func (Tag) TableName() string {
	return "tags"
}

// ToItem 将标签实体转换为接口响应项。
func (t *Tag) ToItem() TagItem {
	return TagItem{
		ID:       t.ID,
		Name:     t.Name,
		UseCount: t.UseCount,
	}
}

// Wallpaper 对应 MySQL wallpapers 表实体。
type Wallpaper struct {
	ID            uint64    `gorm:"primaryKey;autoIncrement"`
	UserID        uint64    `gorm:"not null"`
	CategoryID    uint64    `gorm:"not null"`
	Title         string    `gorm:"type:varchar(80);not null"`
	Description   string    `gorm:"type:varchar(500);not null;default:''"`
	ObjectKey     string    `gorm:"type:varchar(255);not null;uniqueIndex:uk_object_key"`
	Width         uint32    `gorm:"not null"`
	Height        uint32    `gorm:"not null"`
	Orientation   uint8     `gorm:"not null"`
	Is4K          uint8     `gorm:"column:is_4k;not null;default:0"`
	SizeBytes     uint64    `gorm:"not null"`
	Mime          string    `gorm:"type:varchar(32);not null"`
	LikeCount     uint64    `gorm:"not null;default:0"`
	FavCount      uint64    `gorm:"not null;default:0"`
	DownloadCount uint64    `gorm:"not null;default:0"`
	CommentCount  uint64    `gorm:"not null;default:0"`
	Status        uint8     `gorm:"not null;default:1"`
	CreatedAt     time.Time `gorm:"autoCreateTime"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime"`
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

// WallpaperOwnerRecord 为软删鉴权查询的轻量投影结构。
type WallpaperOwnerRecord struct {
	ID     uint64
	UserID uint64
	Status uint8
}

// CalcOrientation 按 ±5% 容差计算壁纸方向（1-横屏 2-竖屏 3-方图）。
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

// Is4KResolution 判断图片是否达到 4K 标准（兼容横屏 3840x2160 与竖屏 2160x3840）。
func Is4KResolution(width, height uint32) bool {
	return max(width, height) >= 3840 && min(width, height) >= 2160
}

// CategoryItem 为壁纸分类响应项。
type CategoryItem struct {
	ID     uint64 `json:"id,string"`
	Slug   string `json:"slug"`
	Name   string `json:"name"`
	SortNo int    `json:"sort_no"`
}

// CategoryList 为全站分类列表响应结构。
type CategoryList struct {
	Items []CategoryItem `json:"items"`
}

// TagItem 为壁纸标签响应项。
type TagItem struct {
	ID       uint64 `json:"id,string"`
	Name     string `json:"name"`
	UseCount uint64 `json:"use_count"`
}

// TagList 为热门或联想标签列表响应结构。
type TagList struct {
	Items []TagItem `json:"items"`
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

// CreateWallpaperRequest 为投稿发布壁纸请求体。
type CreateWallpaperRequest struct {
	Title       string   `json:"title"`
	Description string   `json:"description"`
	CategoryID  string   `json:"category_id"`
	Tags        []string `json:"tags"`
	ObjectKey   string   `json:"object_key"`
}

// WallpaperAuthor 为壁纸详情内嵌作者信息。
type WallpaperAuthor struct {
	ID        uint64 `json:"id,string"`
	Username  string `json:"username"`
	Nickname  string `json:"nickname"`
	AvatarURL string `json:"avatar_url"`
}

// WallpaperDetail 为壁纸详情响应结构。
type WallpaperDetail struct {
	ID            uint64          `json:"id,string"`
	Title         string          `json:"title"`
	Description   string          `json:"description"`
	ThumbURL      string          `json:"thumb_url"`
	RawURL        string          `json:"raw_url"`
	Width         uint32          `json:"width"`
	Height        uint32          `json:"height"`
	Orientation   uint8           `json:"orientation"`
	Is4K          bool            `json:"is_4k"`
	SizeBytes     uint64          `json:"size_bytes"`
	Mime          string          `json:"mime"`
	LikeCount     uint64          `json:"like_count"`
	FavCount      uint64          `json:"fav_count"`
	DownloadCount uint64          `json:"download_count"`
	CommentCount  uint64          `json:"comment_count"`
	Category      CategoryItem    `json:"category"`
	Tags          []TagItem       `json:"tags"`
	Author        WallpaperAuthor `json:"author"`
	CreatedAt     time.Time       `json:"created_at"`
}
