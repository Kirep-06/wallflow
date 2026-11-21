package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/storage"
)

const userLockStripes = 256

type PatchProfileReq struct {
	Nickname  OptionalString `json:"nickname"`
	Bio       OptionalString `json:"bio"`
	AvatarKey OptionalString `json:"avatar_key"`
}

// PostBatchReader 抽象帖子详情批量读取能力（由 PostService 实现）。
type PostBatchReader interface {
	GetPosts(ctx context.Context, ids []uint64) ([]*model.PostDTO, error)
}

type UserService struct {
	users        UserStore
	posts        PostStore
	postReader   PostBatchReader
	storage      storage.Service
	profileCache *cache.MultiLevel
	userCfg      config.UserConfig
	feedCfg      config.FeedConfig
	locks        [userLockStripes]sync.Mutex
	now          func() time.Time
}

func NewUserService(
	users UserStore,
	posts PostStore,
	postReader PostBatchReader,
	st storage.Service,
	profileCache *cache.MultiLevel,
	userCfg config.UserConfig,
	feedCfg config.FeedConfig,
) *UserService {
	return &UserService{
		users:        users,
		posts:        posts,
		postReader:   postReader,
		storage:      st,
		profileCache: profileCache,
		userCfg:      userCfg,
		feedCfg:      feedCfg,
		now:          time.Now,
	}
}

func (s *UserService) userLock(userID uint64) *sync.Mutex {
	return &s.locks[userID%userLockStripes]
}

// InvalidateProfile 在 MySQL 写提交后清理指定用户的 L1/L2 资料缓存（按用户 ID 串行化）。
func (s *UserService) InvalidateProfile(ctx context.Context, userID uint64) {
	mu := s.userLock(userID)
	mu.Lock()
	defer mu.Unlock()

	if err := s.profileCache.Delete(ctx, userID); err != nil {
		slog.WarnContext(ctx, "delete user profile l2 cache failed", "user_id", userID, "err", err)
	}
}

// GetProfile 通过三级缓存读取用户公开资料；按用户 ID 与写后失效串行化。
func (s *UserService) GetProfile(ctx context.Context, userID uint64, _ uint64) (*model.UserProfileDTO, error) {
	if userID == 0 {
		return nil, errno.ErrParam
	}

	mu := s.userLock(userID)
	mu.Lock()
	defer mu.Unlock()

	load := func(ctx context.Context, missing []uint64) (map[uint64][]byte, error) {
		users, err := s.users.GetPublicByIDs(ctx, missing)
		if err != nil {
			return nil, err
		}
		out := make(map[uint64][]byte, len(users))
		for _, u := range users {
			b, err := json.Marshal(model.NewUserProfileCache(u))
			if err != nil {
				return nil, fmt.Errorf("marshal user profile cache: %w", err)
			}
			out[u.ID] = b
		}
		return out, nil
	}
	redisTTL := time.Duration(s.userCfg.ProfileRedisTTL) * time.Second
	ttl := func(uint64) time.Duration { return redisTTL }

	vals, err := s.profileCache.GetMany(ctx, []uint64{userID}, load, ttl)
	if err != nil {
		return nil, err
	}
	raw, ok := vals[userID]
	if !ok {
		return nil, errno.ErrUserNotFound
	}
	var pc model.UserProfileCache
	if err := json.Unmarshal(raw, &pc); err != nil {
		slog.WarnContext(ctx, "bad user profile cache", "user_id", userID, "err", err)
		return nil, fmt.Errorf("unmarshal user profile cache: %w", err)
	}
	return s.cacheToDTO(&pc), nil
}

// UpdateProfile 执行部分字段更新：校验 -> 字段级 UPDATE -> 按主键回读 -> 清理 L1/L2 缓存。
func (s *UserService) UpdateProfile(ctx context.Context, userID uint64, req *PatchProfileReq) (*model.UserProfileDTO, error) {
	if userID == 0 {
		return nil, errno.ErrUnauth
	}
	if !req.Nickname.Set && !req.Bio.Set && !req.AvatarKey.Set {
		return nil, errno.ErrParam.WithMsg("至少提供一个需要更新的字段")
	}

	updates := make(map[string]any, 4)

	if req.Nickname.Set {
		if req.Nickname.Null {
			return nil, errno.ErrParam.WithMsg("昵称不能为 null")
		}
		if err := validateNickname(req.Nickname.Value); err != nil {
			return nil, err
		}
		updates["nickname"] = req.Nickname.Value
	}

	if req.Bio.Set {
		if req.Bio.Null {
			return nil, errno.ErrParam.WithMsg("简介不能为 null")
		}
		if !utf8.ValidString(req.Bio.Value) || utf8.RuneCountInString(req.Bio.Value) > maxBioRunes {
			return nil, errno.ErrParam.WithMsg("简介不能超过 200 个字符")
		}
		updates["bio"] = req.Bio.Value
	}

	if req.AvatarKey.Set {
		if req.AvatarKey.Null {
			return nil, errno.ErrParam.WithMsg("头像 Key 不能为 null")
		}
		key := req.AvatarKey.Value
		if key != "" {
			if !storage.ValidateAvatarKey(userID, key) {
				return nil, errno.ErrParam.WithMsg("头像 Key 无效")
			}
			info, err := s.storage.Stat(ctx, key)
			if errors.Is(err, storage.ErrObjectNotFound) {
				return nil, errno.ErrParam.WithMsg("头像文件不存在，请重新上传")
			}
			if err != nil {
				return nil, err
			}
			if !slices.Contains(storage.AllowedMimes, info.MimeType) || info.Size < 1 || info.Size > storage.AvatarMaxSizeBytes {
				return nil, errno.ErrParam.WithMsg("头像文件类型或大小不符合要求")
			}
		}
		updates["avatar_key"] = key
	}

	updates["updated_at"] = s.now()

	if err := s.users.UpdateFields(ctx, userID, updates); err != nil {
		return nil, err
	}

	pub, err := s.users.GetPublicByID(ctx, userID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, errno.ErrUserNotFound
		}
		return nil, err
	}

	// MySQL 提交后删缓存；Redis 删除失败只记录日志并已清理本地值，接口仍返回已提交结果。
	s.InvalidateProfile(ctx, userID)

	return s.publicToDTO(pub), nil
}

// ListUserPosts 按游标分页返回指定用户的正常帖子列表。
func (s *UserService) ListUserPosts(ctx context.Context, targetUserID uint64, rawCursor string, limit int) (*model.FeedPage, error) {
	if targetUserID == 0 {
		return nil, errno.ErrParam
	}
	if limit == 0 {
		limit = s.feedCfg.PageDefault
		if limit <= 0 {
			limit = 20
		}
	}
	maxLimit := s.feedCfg.PageMax
	if maxLimit <= 0 {
		maxLimit = 50
	}
	if limit < 1 || limit > maxLimit {
		return nil, errno.ErrParam
	}

	cursorID, err := cursor.DecodeID(rawCursor)
	if err != nil {
		return nil, errno.ErrBadCursor
	}

	// 确认目标用户存在（复用资料缓存）；不存在返回 20003。
	if _, err := s.GetProfile(ctx, targetUserID, 0); err != nil {
		return nil, err
	}

	ids, err := s.posts.ListUserIDsBefore(ctx, targetUserID, cursorID, limit+1)
	if err != nil {
		return nil, err
	}

	hasMore := len(ids) > limit
	if hasMore {
		ids = ids[:limit]
	}

	nextCursor := ""
	if hasMore && len(ids) > 0 {
		// 游标取本次扫描的最后一个 ID，即使该帖后续被并发删除也不重复扫描。
		nextCursor = cursor.EncodeID(ids[len(ids)-1])
	}

	items, err := s.postReader.GetPosts(ctx, ids)
	if err != nil {
		return nil, err
	}
	if items == nil {
		items = []*model.PostDTO{}
	}
	return &model.FeedPage{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (s *UserService) avatarURL(key string) string {
	if key == "" {
		key = s.userCfg.DefaultAvatarKey
	}
	return s.storage.URL(key)
}

func (s *UserService) cacheToDTO(pc *model.UserProfileCache) *model.UserProfileDTO {
	return &model.UserProfileDTO{
		ID:             pc.ID,
		Username:       pc.Username,
		Nickname:       pc.Nickname,
		AvatarURL:      s.avatarURL(pc.AvatarKey),
		Bio:            pc.Bio,
		FollowingCount: 0,
		FollowerCount:  0,
		IsFollowing:    false,
		CreatedAt:      pc.CreatedAt,
	}
}

func (s *UserService) publicToDTO(u *model.UserPublic) *model.UserProfileDTO {
	return &model.UserProfileDTO{
		ID:             u.ID,
		Username:       u.Username,
		Nickname:       u.Nickname,
		AvatarURL:      s.avatarURL(u.AvatarKey),
		Bio:            u.Bio,
		FollowingCount: 0,
		FollowerCount:  0,
		IsFollowing:    false,
		CreatedAt:      u.CreatedAt.UnixMilli(),
	}
}
