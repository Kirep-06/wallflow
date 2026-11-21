package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/sync/errgroup"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/storage"
)

const (
	MaxContentRunes = 2000
	MaxImages       = 9
)

type CreatePostReq struct {
	Content string           `json:"content"`
	Images  []CreateImageReq `json:"images"`
}

// CreateImageReq 对应七牛上传成功后返回的 returnBody。
type CreateImageReq struct {
	Key    string `json:"key"`
	Width  uint32 `json:"width"`
	Height uint32 `json:"height"`
	Size   int64  `json:"size"`
	Mime   string `json:"mime"`
}

// OutboxNotifier 在发帖事务提交后非阻塞唤醒 OutboxRelay，实现毫秒级投递。
type OutboxNotifier interface {
	Notify()
}

type PostService struct {
	store          PostStore
	feed           *repository.LatestFeedRepo
	storage        storage.Service
	detail         *cache.MultiLevel
	cfg            config.CacheConfig
	outboxExchange string
	outboxNotifier OutboxNotifier
}

func NewPostService(store PostStore, feed *repository.LatestFeedRepo, st storage.Service, detail *cache.MultiLevel, cfg config.CacheConfig) *PostService {
	return &PostService{store: store, feed: feed, storage: st, detail: detail, cfg: cfg}
}

// SetOutbox 装配发帖事件的 Outbox Exchange 名称与提交后唤醒器。
func (s *PostService) SetOutbox(exchange string, notifier OutboxNotifier) {
	s.outboxExchange = exchange
	s.outboxNotifier = notifier
}

func (s *PostService) UploadToken(ctx context.Context, scene storage.UploadScene, userID uint64) (*storage.UploadToken, error) {
	if scene == "" {
		scene = storage.UploadScenePost
	}
	if scene != storage.UploadScenePost && scene != storage.UploadSceneAvatar {
		return nil, errno.ErrParam.WithMsg("不支持的上传场景")
	}
	return s.storage.UploadToken(ctx, scene, userID)
}

// Create 发帖：校验 → 确认对象存在 → 事务写 MySQL → 尽力写入最新流热窗口。
func (s *PostService) Create(ctx context.Context, userID uint64, req *CreatePostReq) (*model.PostDTO, error) {
	if utf8.RuneCountInString(req.Content) > MaxContentRunes {
		return nil, errno.ErrPostContentLong
	}
	if n := len(req.Images); n < 1 || n > MaxImages {
		return nil, errno.ErrPostImageCount
	}
	seen := make(map[string]struct{}, len(req.Images))
	for _, img := range req.Images {
		if !strings.HasPrefix(img.Key, storage.KeyPrefix) || strings.Contains(img.Key, "..") || img.Width == 0 || img.Height == 0 {
			return nil, errno.ErrPostImageInvalid
		}
		if _, dup := seen[img.Key]; dup {
			return nil, errno.ErrPostImageInvalid.WithMsg("图片重复")
		}
		seen[img.Key] = struct{}{}
	}
	infos, err := s.statImages(ctx, req.Images)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	post := &model.Post{
		UserID:     userID,
		Content:    req.Content,
		ImageCount: uint8(len(req.Images)),
		Status:     model.PostStatusNormal,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	for i, img := range req.Images {
		post.Images = append(post.Images, model.PostImage{
			SortNo:    uint8(i),
			ObjectKey: img.Key,
			Width:     img.Width,
			Height:    img.Height,
			SizeBytes: uint32(infos[i].Size), // 以对象存储返回值为准
			Mime:      infos[i].MimeType,
			CreatedAt: now,
		})
	}
	var buildEvent func(p *model.Post) (*model.OutboxEvent, error)
	if s.outboxExchange != "" {
		exchange := s.outboxExchange
		buildEvent = func(p *model.Post) (*model.OutboxEvent, error) {
			eventID := fmt.Sprintf("%s:%d", model.EventTypePostCreated, p.ID)
			payload, err := json.Marshal(model.PostCreatedEvent{
				EventID:   eventID,
				EventType: model.EventTypePostCreated,
				PostID:    p.ID,
				AuthorID:  p.UserID,
				CreatedAt: p.CreatedAt.UnixMilli(),
			})
			if err != nil {
				return nil, fmt.Errorf("marshal post created event: %w", err)
			}
			return &model.OutboxEvent{
				EventID:      eventID,
				ExchangeName: exchange,
				RoutingKey:   model.EventTypePostCreated,
				Payload:      string(payload),
				Status:       model.OutboxStatusPending,
				RetryCount:   0,
				NextRetryAt:  p.CreatedAt,
				CreatedAt:    p.CreatedAt,
				UpdatedAt:    p.CreatedAt,
			}, nil
		}
	}
	if err := s.store.CreateWithOutbox(ctx, post, buildEvent); err != nil {
		return nil, err
	}

	// MySQL 是唯一真实数据源：热窗口写失败只记日志，由 Worker 对账补齐。
	if _, err := s.feed.Add(ctx, post.ID); err != nil {
		slog.WarnContext(ctx, "add post to latest feed failed", "post_id", post.ID, "err", err)
	}
	if s.outboxNotifier != nil {
		s.outboxNotifier.Notify()
	}
	return s.toDTO(model.NewPostCache(post)), nil
}

// statImages 并发确认图片对象存在且类型合法（并发度受图片数上限 9 约束）。
func (s *PostService) statImages(ctx context.Context, imgs []CreateImageReq) ([]*storage.ObjectInfo, error) {
	infos := make([]*storage.ObjectInfo, len(imgs))
	g, gctx := errgroup.WithContext(ctx)
	for i, img := range imgs {
		g.Go(func() error {
			info, err := s.storage.Stat(gctx, img.Key)
			if errors.Is(err, storage.ErrObjectNotFound) {
				return errno.ErrPostImageInvalid.WithMsg("图片不存在，请重新上传")
			}
			if err != nil {
				return err
			}
			if !slices.Contains(storage.AllowedMimes, info.MimeType) {
				return errno.ErrPostImageInvalid.WithMsg("不支持的图片格式")
			}
			infos[i] = info
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return infos, nil
}

// Get 返回单个帖子详情。
func (s *PostService) Get(ctx context.Context, id uint64) (*model.PostDTO, error) {
	posts, err := s.GetPosts(ctx, []uint64{id})
	if err != nil {
		return nil, err
	}
	if len(posts) == 0 {
		return nil, errno.ErrPostNotFound
	}
	return posts[0], nil
}

// GetPosts 经三级缓存批量加载帖子详情，按 ids 原顺序输出，跳过不存在或非正常状态的帖子。
func (s *PostService) GetPosts(ctx context.Context, ids []uint64) ([]*model.PostDTO, error) {
	if len(ids) == 0 {
		return []*model.PostDTO{}, nil
	}
	// hotMin 在回源时取一次，用于区分冷/热帖的 L2 TTL；load 与 ttl 由同一次 singleflight 调用执行。
	var hotMin uint64
	load := func(ctx context.Context, missing []uint64) (map[uint64][]byte, error) {
		if m, err := s.feed.MinID(ctx); err == nil {
			hotMin = m
		}
		posts, err := s.store.GetByIDs(ctx, missing)
		if err != nil {
			return nil, err
		}
		out := make(map[uint64][]byte, len(posts))
		for _, p := range posts {
			b, err := json.Marshal(model.NewPostCache(p))
			if err != nil {
				return nil, fmt.Errorf("marshal post cache: %w", err)
			}
			out[p.ID] = b
		}
		return out, nil
	}
	ttl := func(id uint64) time.Duration {
		if hotMin > 0 && id >= hotMin {
			return time.Duration(s.cfg.RedisHotTTL) * time.Second
		}
		return time.Duration(s.cfg.RedisColdTTL) * time.Second
	}

	vals, err := s.detail.GetMany(ctx, ids, load, ttl)
	if err != nil {
		return nil, err
	}
	out := make([]*model.PostDTO, 0, len(ids))
	for _, id := range ids {
		raw, ok := vals[id]
		if !ok {
			continue
		}
		var pc model.PostCache
		if err := json.Unmarshal(raw, &pc); err != nil {
			slog.WarnContext(ctx, "bad post cache", "post_id", id, "err", err)
			continue
		}
		if pc.Status != model.PostStatusNormal {
			continue
		}
		out = append(out, s.toDTO(&pc))
	}
	return out, nil
}

func (s *PostService) toDTO(pc *model.PostCache) *model.PostDTO {
	dto := &model.PostDTO{
		ID:        pc.ID,
		UserID:    pc.UserID,
		Content:   pc.Content,
		CreatedAt: pc.CreatedAt,
		Images:    make([]model.ImageDTO, 0, len(pc.Images)),
	}
	for _, img := range pc.Images {
		dto.Images = append(dto.Images, model.ImageDTO{
			URL:      s.storage.URL(img.Key),
			ThumbURL: s.storage.ThumbURL(img.Key),
			Width:    img.Width,
			Height:   img.Height,
		})
	}
	return dto
}
