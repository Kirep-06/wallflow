package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	authSessionKeyPrefix = "auth:session:"
	fieldUserID          = "user_id"
	fieldRefreshSHA256   = "refresh_sha256"
	fieldExpiresAt       = "expires_at"
)

var (
	ErrSessionNotFound        = errors.New("repository: auth session not found")
	ErrSessionRefreshMismatch = errors.New("repository: auth session refresh hash mismatch")
)

// updateRefreshHashScript 原子比对旧摘要并更新 refresh_sha256，保持原有绝对 TTL 不变且不复活已注销会话。
var updateRefreshHashScript = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], 'refresh_sha256')
if not cur then
	return -1
end
if cur ~= ARGV[1] then
	return 0
end
redis.call('HSET', KEYS[1], 'refresh_sha256', ARGV[2])
return 1
`)

// AuthSession 表示 Redis 中存储的轻量登录会话。
type AuthSession struct {
	SessionID     string
	UserID        uint64
	RefreshSHA256 string
	ExpiresAt     time.Time
}

// AuthSessionRepository 定义 Redis 登录会话仓储接口（无 L1 缓存）。
type AuthSessionRepository interface {
	Create(ctx context.Context, session AuthSession) error
	GetAuthCheck(ctx context.Context, sid string) (userID uint64, expiresAt time.Time, err error)
	Get(ctx context.Context, sid string) (*AuthSession, error)
	UpdateRefreshHash(ctx context.Context, sid, oldHash, newHash string) error
	Delete(ctx context.Context, sid string) error
}

type authSessionRepository struct {
	rdb *redis.Client
}

// NewAuthSessionRepository 构造基于 Redis 的会话仓储实例。
func NewAuthSessionRepository(rdb *redis.Client) AuthSessionRepository {
	return &authSessionRepository{rdb: rdb}
}

func authSessionKey(sid string) string {
	return authSessionKeyPrefix + sid
}

func (r *authSessionRepository) Create(ctx context.Context, session AuthSession) error {
	key := authSessionKey(session.SessionID)
	expUnix := session.ExpiresAt.UTC().Unix()

	pipe := r.rdb.TxPipeline()
	pipe.HSet(ctx, key,
		fieldUserID, strconv.FormatUint(session.UserID, 10),
		fieldRefreshSHA256, session.RefreshSHA256,
		fieldExpiresAt, strconv.FormatInt(expUnix, 10),
	)
	pipe.ExpireAt(ctx, key, time.Unix(expUnix, 0).UTC())
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("auth session repo create: %w", err)
	}
	return nil
}

func (r *authSessionRepository) GetAuthCheck(ctx context.Context, sid string) (uint64, time.Time, error) {
	vals, err := r.rdb.HMGet(ctx, authSessionKey(sid), fieldUserID, fieldExpiresAt).Result()
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("auth session repo hmget check: %w", err)
	}
	if len(vals) != 2 || vals[0] == nil || vals[1] == nil {
		return 0, time.Time{}, ErrSessionNotFound
	}

	uidStr, ok1 := vals[0].(string)
	expStr, ok2 := vals[1].(string)
	if !ok1 || !ok2 {
		return 0, time.Time{}, ErrSessionNotFound
	}

	uid, err := strconv.ParseUint(uidStr, 10, 64)
	if err != nil || uid == 0 {
		return 0, time.Time{}, ErrSessionNotFound
	}
	expUnix, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil || expUnix <= 0 {
		return 0, time.Time{}, ErrSessionNotFound
	}

	return uid, time.Unix(expUnix, 0).UTC(), nil
}

func (r *authSessionRepository) Get(ctx context.Context, sid string) (*AuthSession, error) {
	vals, err := r.rdb.HMGet(ctx, authSessionKey(sid), fieldUserID, fieldRefreshSHA256, fieldExpiresAt).Result()
	if err != nil {
		return nil, fmt.Errorf("auth session repo hmget: %w", err)
	}
	if len(vals) != 3 || vals[0] == nil || vals[1] == nil || vals[2] == nil {
		return nil, ErrSessionNotFound
	}

	uidStr, ok1 := vals[0].(string)
	hashStr, ok2 := vals[1].(string)
	expStr, ok3 := vals[2].(string)
	if !ok1 || !ok2 || !ok3 || hashStr == "" {
		return nil, ErrSessionNotFound
	}

	uid, err := strconv.ParseUint(uidStr, 10, 64)
	if err != nil || uid == 0 {
		return nil, ErrSessionNotFound
	}
	expUnix, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil || expUnix <= 0 {
		return nil, ErrSessionNotFound
	}

	return &AuthSession{
		SessionID:     sid,
		UserID:        uid,
		RefreshSHA256: hashStr,
		ExpiresAt:     time.Unix(expUnix, 0).UTC(),
	}, nil
}

func (r *authSessionRepository) UpdateRefreshHash(ctx context.Context, sid, oldHash, newHash string) error {
	res, err := updateRefreshHashScript.Run(ctx, r.rdb, []string{authSessionKey(sid)}, oldHash, newHash).Int()
	if err != nil {
		return fmt.Errorf("auth session repo update refresh hash: %w", err)
	}
	switch res {
	case 1:
		return nil
	case 0:
		return ErrSessionRefreshMismatch
	default:
		return ErrSessionNotFound
	}
}

func (r *authSessionRepository) Delete(ctx context.Context, sid string) error {
	if err := r.rdb.Del(ctx, authSessionKey(sid)).Err(); err != nil {
		return fmt.Errorf("auth session repo delete: %w", err)
	}
	return nil
}
