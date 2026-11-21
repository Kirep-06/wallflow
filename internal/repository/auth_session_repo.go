package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const (
	authSessionPrefix = "auth:session:"
	MaxRotations      = 1024
)

var ErrSessionExists = errors.New("auth session already exists")

type RotateResult int

const (
	RotateOK RotateResult = iota + 1
	RotateMissingOrExpired
	RotateReused
	RotateMismatch
	RotateMaxRotations
	RotateUserMismatch
)

// SessionAuthState 是鉴权时从 Redis 读取的会话元数据。
type SessionAuthState struct {
	Exists    bool
	UserID    uint64
	ExpiresAt int64
}

// SessionRefreshState 是刷新预检时从 Redis 读取的会话元数据。
type SessionRefreshState struct {
	Exists        bool
	UserID        uint64
	ExpiresAt     int64
	RefreshSHA256 string
	Rotations     int
}

// createSessionScript 检查新 sid 未被使用，原子写入会话字段与绝对过期时间。
var createSessionScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
local exp = tonumber(ARGV[2])
local now = tonumber(ARGV[4])
if not exp or not now or exp <= now then
  return -1
end
redis.call('HSET', KEYS[1],
  'user_id', ARGV[1],
  'expires_at', ARGV[2],
  'refresh_sha256', ARGV[3],
  'rotations', '0'
)
redis.call('EXPIREAT', KEYS[1], exp)
return 1
`)

// rotateSessionScript 原子校验并轮换 Refresh Token，同时检测已消费凭证重用与轮换上限。
// 注意：单次 HSET 不会重置原有 Key 的 TTL。
var rotateSessionScript = redis.NewScript(`
local vals = redis.call('HMGET', KEYS[1], 'user_id', 'expires_at', 'refresh_sha256', 'rotations')
local uid = vals[1]
local exp = tonumber(vals[2])
local cur = vals[3]
local rot = tonumber(vals[4]) or 0
local now = tonumber(ARGV[4])
local maxRot = tonumber(ARGV[5])

if not uid or not exp or not cur then
  return 0
end
if not now or exp <= now then
  return 0
end

local oldHash = ARGV[2]
if cur ~= oldHash then
  if redis.call('HEXISTS', KEYS[1], 'used:' .. oldHash) == 1 then
    redis.call('DEL', KEYS[1])
    return -1
  end
  return -2
end

if ARGV[3] == '' then
  return -2
end

if ARGV[1] ~= '' and uid ~= ARGV[1] then
  return -4
end

if rot >= maxRot then
  redis.call('DEL', KEYS[1])
  return -3
end

redis.call('HSET', KEYS[1],
  'used:' .. oldHash, '1',
  'refresh_sha256', ARGV[3],
  'rotations', tostring(rot + 1)
)
return 1
`)

// AuthSessionRepo 管理 Redis 中的认证会话（auth:session:{sid}）。
// 调用方应传入关闭自动重试（MaxRetries: -1）的 Redis 客户端，防止轮换脚本被底层自动重发。
type AuthSessionRepo struct {
	rdb redis.UniversalClient
}

func NewAuthSessionRepo(rdb redis.UniversalClient) *AuthSessionRepo {
	return &AuthSessionRepo{rdb: rdb}
}

func authSessionKey(sid string) string { return authSessionPrefix + sid }

// Create 原子创建新会话，写入失败或 sid 冲突时不返回成功。
func (r *AuthSessionRepo) Create(ctx context.Context, sid string, userID uint64, expiresAt int64, refreshSHA256 string, now int64) error {
	res, err := createSessionScript.Run(
		ctx,
		r.rdb,
		[]string{authSessionKey(sid)},
		strconv.FormatUint(userID, 10),
		strconv.FormatInt(expiresAt, 10),
		refreshSHA256,
		strconv.FormatInt(now, 10),
	).Int()
	if err != nil {
		return fmt.Errorf("create auth session: %w", err)
	}
	switch res {
	case 1:
		return nil
	case 0:
		return ErrSessionExists
	default:
		return fmt.Errorf("create auth session rejected: code %d", res)
	}
}

// GetForAuth 鉴权时读取会话的 user_id 和 expires_at。
func (r *AuthSessionRepo) GetForAuth(ctx context.Context, sid string) (SessionAuthState, error) {
	vals, err := r.rdb.HMGet(ctx, authSessionKey(sid), "user_id", "expires_at").Result()
	if err != nil {
		return SessionAuthState{}, fmt.Errorf("read auth session: %w", err)
	}
	if len(vals) != 2 || vals[0] == nil || vals[1] == nil {
		return SessionAuthState{}, nil
	}
	uidStr, ok1 := vals[0].(string)
	expStr, ok2 := vals[1].(string)
	if !ok1 || !ok2 {
		return SessionAuthState{}, nil
	}
	uid, err1 := strconv.ParseUint(uidStr, 10, 64)
	exp, err2 := strconv.ParseInt(expStr, 10, 64)
	if err1 != nil || err2 != nil || uid == 0 {
		return SessionAuthState{}, nil
	}
	return SessionAuthState{Exists: true, UserID: uid, ExpiresAt: exp}, nil
}

// PeekForRefresh 在刷新阶段预检会话元数据。
func (r *AuthSessionRepo) PeekForRefresh(ctx context.Context, sid string) (SessionRefreshState, error) {
	vals, err := r.rdb.HMGet(ctx, authSessionKey(sid), "user_id", "expires_at", "refresh_sha256", "rotations").Result()
	if err != nil {
		return SessionRefreshState{}, fmt.Errorf("peek auth session: %w", err)
	}
	if len(vals) != 4 || vals[0] == nil || vals[1] == nil || vals[2] == nil {
		return SessionRefreshState{}, nil
	}
	uidStr, ok1 := vals[0].(string)
	expStr, ok2 := vals[1].(string)
	hashStr, ok3 := vals[2].(string)
	if !ok1 || !ok2 || !ok3 {
		return SessionRefreshState{}, nil
	}
	uid, err1 := strconv.ParseUint(uidStr, 10, 64)
	exp, err2 := strconv.ParseInt(expStr, 10, 64)
	if err1 != nil || err2 != nil || uid == 0 || hashStr == "" {
		return SessionRefreshState{}, nil
	}
	rot := 0
	if vals[3] != nil {
		if rotStr, ok := vals[3].(string); ok {
			if n, err := strconv.Atoi(rotStr); err == nil {
				rot = n
			}
		}
	}
	return SessionRefreshState{
		Exists:        true,
		UserID:        uid,
		ExpiresAt:     exp,
		RefreshSHA256: hashStr,
		Rotations:     rot,
	}, nil
}

// Rotate 原子执行轮换或旧凭证重用检测。当 newSHA256 为空时仅做重用检测（命中 used:<hash> 则删会话）。
func (r *AuthSessionRepo) Rotate(ctx context.Context, sid string, expectedUserID uint64, oldSHA256, newSHA256 string, now int64, maxRotations int) (RotateResult, error) {
	uidArg := ""
	if expectedUserID > 0 {
		uidArg = strconv.FormatUint(expectedUserID, 10)
	}
	res, err := rotateSessionScript.Run(
		ctx,
		r.rdb,
		[]string{authSessionKey(sid)},
		uidArg,
		oldSHA256,
		newSHA256,
		strconv.FormatInt(now, 10),
		strconv.Itoa(maxRotations),
	).Int()
	if err != nil {
		return 0, fmt.Errorf("rotate auth session: %w", err)
	}
	switch res {
	case 1:
		return RotateOK, nil
	case 0:
		return RotateMissingOrExpired, nil
	case -1:
		return RotateReused, nil
	case -2:
		return RotateMismatch, nil
	case -3:
		return RotateMaxRotations, nil
	case -4:
		return RotateUserMismatch, nil
	default:
		return 0, fmt.Errorf("unexpected rotate result: %d", res)
	}
}

// Delete 撤销当前会话。
func (r *AuthSessionRepo) Delete(ctx context.Context, sid string) error {
	if err := r.rdb.Del(ctx, authSessionKey(sid)).Err(); err != nil {
		return fmt.Errorf("delete auth session: %w", err)
	}
	return nil
}
