package service

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/bcrypt"

	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/jwtutil"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/storage"
)

const (
	minPasswordRunes = 8
	maxPasswordRunes = 64
	maxPasswordBytes = 72 // bcrypt 字节上限，超长直接拒绝，禁止截断
	maxNicknameRunes = 32
	maxBioRunes      = 200
)

var usernamePattern = regexp.MustCompile(`^[a-zA-Z0-9_]{3,32}$`)

// OptionalString 区分 JSON 字段的“省略”、“显式 null”与字符串值。
type OptionalString struct {
	Set   bool
	Null  bool
	Value string
}

func (o *OptionalString) UnmarshalJSON(data []byte) error {
	o.Set = true
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		o.Null = true
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	o.Value = s
	return nil
}

type RegisterReq struct {
	Username string         `json:"username"`
	Password string         `json:"password"`
	Nickname OptionalString `json:"nickname"`
}

type LoginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type RefreshReq struct {
	RefreshToken string `json:"refresh_token"`
}

// SessionStore 抽象 Redis 会话仓储，便于单元测试注入故障。
type SessionStore interface {
	Create(ctx context.Context, sid string, userID uint64, expiresAt int64, refreshSHA256 string, now int64) error
	GetForAuth(ctx context.Context, sid string) (repository.SessionAuthState, error)
	PeekForRefresh(ctx context.Context, sid string) (repository.SessionRefreshState, error)
	Rotate(ctx context.Context, sid string, expectedUserID uint64, oldSHA256, newSHA256 string, now int64, maxRotations int) (repository.RotateResult, error)
	Delete(ctx context.Context, sid string) error
}

// ProfileInvalidator 在账号创建提交后清理可能存在的空值缓存。
type ProfileInvalidator interface {
	InvalidateProfile(ctx context.Context, userID uint64)
}

type AuthService struct {
	users            UserStore
	sessions         SessionStore
	signer           *jwtutil.Signer
	storage          storage.Service
	invalidator      ProfileInvalidator
	cfg              config.AuthConfig
	defaultAvatarKey string
	dummyHash        []byte
	maxRotations     int
	now              func() time.Time
}

func NewAuthService(
	users UserStore,
	sessions SessionStore,
	st storage.Service,
	invalidator ProfileInvalidator,
	authCfg config.AuthConfig,
	userCfg config.UserConfig,
) (*AuthService, error) {
	if err := authCfg.Validate(); err != nil {
		return nil, err
	}
	signer, err := jwtutil.NewSigner(authCfg.JWTSecret, authCfg.Issuer)
	if err != nil {
		return nil, err
	}
	dummy, err := bcrypt.GenerateFromPassword([]byte("wallpaper-feed-dummy-password"), authCfg.BcryptCost)
	if err != nil {
		return nil, fmt.Errorf("init dummy bcrypt hash: %w", err)
	}
	return &AuthService{
		users:            users,
		sessions:         sessions,
		signer:           signer,
		storage:          st,
		invalidator:      invalidator,
		cfg:              authCfg,
		defaultAvatarKey: userCfg.DefaultAvatarKey,
		dummyHash:        dummy,
		maxRotations:     repository.MaxRotations,
		now:              time.Now,
	}, nil
}

// Register 注册新用户并自动建立独立会话。
func (s *AuthService) Register(ctx context.Context, req *RegisterReq) (*model.AuthTokenData, error) {
	if !usernamePattern.MatchString(req.Username) {
		return nil, errno.ErrParam.WithMsg("用户名需为 3~32 位字母、数字或下划线")
	}
	if err := validatePasswordForRegister(req.Password); err != nil {
		return nil, err
	}
	nickname := req.Username
	if req.Nickname.Set {
		if req.Nickname.Null {
			return nil, errno.ErrParam.WithMsg("昵称不能为空")
		}
		if err := validateNickname(req.Nickname.Value); err != nil {
			return nil, err
		}
		nickname = req.Nickname.Value
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), s.cfg.BcryptCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	now := s.now()
	u := &model.User{
		Username:     req.Username,
		PasswordHash: string(hash),
		Nickname:     nickname,
		AvatarKey:    "",
		Bio:          "",
		Status:       model.UserStatusNormal,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := s.users.Create(ctx, u); err != nil {
		if errors.Is(err, repository.ErrDuplicateUsername) {
			return nil, errno.ErrUsernameTaken
		}
		return nil, err
	}

	// MySQL 提交后清理可能存在的空值缓存标记。
	if s.invalidator != nil {
		s.invalidator.InvalidateProfile(ctx, u.ID)
	}

	return s.createSession(ctx, model.UserSummaryDTO{
		ID:        u.ID,
		Username:  u.Username,
		Nickname:  u.Nickname,
		AvatarURL: s.avatarURL(u.AvatarKey),
	})
}

// Login 校验账号密码并新建会话；缺失账号与密码错误统一走同成本 bcrypt 比较并返回 20002。
func (s *AuthService) Login(ctx context.Context, req *LoginReq) (*model.AuthTokenData, error) {
	if req.Username == "" || req.Password == "" || !utf8.ValidString(req.Password) {
		return nil, errno.ErrParam
	}
	if len(req.Password) > maxPasswordBytes {
		return nil, errno.ErrParam.WithMsg("密码长度超出限制")
	}

	rec, err := s.users.GetAuthByUsername(ctx, req.Username)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			_ = bcrypt.CompareHashAndPassword(s.dummyHash, []byte(req.Password))
			return nil, errno.ErrInvalidCredentials
		}
		return nil, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(rec.PasswordHash), []byte(req.Password)); err != nil {
		return nil, errno.ErrInvalidCredentials
	}
	if rec.Status != model.UserStatusNormal {
		return nil, errno.ErrForbidden.WithMsg("账号已被封禁")
	}

	return s.createSession(ctx, model.UserSummaryDTO{
		ID:        rec.ID,
		Username:  rec.Username,
		Nickname:  rec.Nickname,
		AvatarURL: s.avatarURL(rec.AvatarKey),
	})
}

// Refresh 原子轮换双 Token；继承原会话固定到期时间，检测旧凭证重用并撤销会话。
func (s *AuthService) Refresh(ctx context.Context, req *RefreshReq) (*model.AuthTokenData, error) {
	if req.RefreshToken == "" {
		return nil, errno.ErrParam
	}
	sid, ok := jwtutil.ParseRefreshToken(req.RefreshToken)
	if !ok {
		return nil, errno.ErrSessionExpired
	}
	oldHash := jwtutil.HashToken(req.RefreshToken)

	now := s.now()
	nowUnix := now.Unix()

	state, err := s.sessions.PeekForRefresh(ctx, sid)
	if err != nil {
		return nil, err
	}
	if !state.Exists || state.ExpiresAt <= nowUnix {
		return nil, errno.ErrSessionExpired
	}

	// 摘要不匹配当前凭证时，仍必须走轮换脚本区分“已消费旧凭证（撤销会话）”与“随机凭证（不伤会话）”。
	if subtle.ConstantTimeCompare([]byte(state.RefreshSHA256), []byte(oldHash)) != 1 {
		if _, err := s.sessions.Rotate(ctx, sid, 0, oldHash, "", nowUnix, s.maxRotations); err != nil {
			return nil, err
		}
		return nil, errno.ErrSessionExpired
	}

	rec, err := s.users.GetStatusByID(ctx, state.UserID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			if derr := s.sessions.Delete(ctx, sid); derr != nil {
				return nil, derr
			}
			return nil, errno.ErrSessionExpired
		}
		return nil, err
	}
	if rec.Status != model.UserStatusNormal {
		if derr := s.sessions.Delete(ctx, sid); derr != nil {
			return nil, derr
		}
		return nil, errno.ErrForbidden.WithMsg("账号已被封禁")
	}

	issueNow := s.now()
	issueNowUnix := issueNow.Unix()
	if state.ExpiresAt <= issueNowUnix {
		return nil, errno.ErrSessionExpired
	}
	accessExpUnix := min(issueNowUnix+int64(s.cfg.AccessTokenTTL), state.ExpiresAt)
	if accessExpUnix <= issueNowUnix {
		return nil, errno.ErrSessionExpired
	}

	newAccess, err := s.signer.Issue(rec.ID, sid, time.Unix(issueNowUnix, 0), time.Unix(accessExpUnix, 0))
	if err != nil {
		return nil, err
	}
	newRefresh, err := jwtutil.GenerateRefreshToken(sid)
	if err != nil {
		return nil, fmt.Errorf("generate refresh token: %w", err)
	}
	newHash := jwtutil.HashToken(newRefresh)

	res, err := s.sessions.Rotate(ctx, sid, rec.ID, oldHash, newHash, issueNowUnix, s.maxRotations)
	if err != nil {
		return nil, err
	}
	if res != repository.RotateOK {
		return nil, errno.ErrSessionExpired
	}

	return &model.AuthTokenData{
		AccessToken:  newAccess,
		RefreshToken: newRefresh,
		ExpiresIn:    accessExpUnix - issueNowUnix,
		User: model.UserSummaryDTO{
			ID:        rec.ID,
			Username:  rec.Username,
			Nickname:  rec.Nickname,
			AvatarURL: s.avatarURL(rec.AvatarKey),
		},
	}, nil
}

// Logout 凭已校验 Access Token 的 sid 撤销当前会话。
func (s *AuthService) Logout(ctx context.Context, sid string) error {
	if !jwtutil.ValidateSessionID(sid) {
		return errno.ErrUnauth
	}
	return s.sessions.Delete(ctx, sid)
}

// Authenticate 先本地验签 JWT，再读取 Redis 会话确认未撤销、未过期且用户一致。
func (s *AuthService) Authenticate(ctx context.Context, rawToken string) (uint64, string, error) {
	now := s.now()
	parsed, err := s.signer.Parse(rawToken, now)
	if err != nil {
		return 0, "", errno.ErrUnauth
	}
	state, err := s.sessions.GetForAuth(ctx, parsed.SID)
	if err != nil {
		return 0, "", err
	}
	if !state.Exists || state.UserID != parsed.UserID || state.ExpiresAt <= now.Unix() || parsed.ExpiresAt > state.ExpiresAt {
		return 0, "", errno.ErrUnauth
	}
	return parsed.UserID, parsed.SID, nil
}

func (s *AuthService) createSession(ctx context.Context, user model.UserSummaryDTO) (*model.AuthTokenData, error) {
	const maxAttempts = 3
	for range maxAttempts {
		nowUnix := s.now().Unix()
		sessionExpUnix := nowUnix + int64(s.cfg.RefreshTokenTTL)
		accessExpUnix := min(nowUnix+int64(s.cfg.AccessTokenTTL), sessionExpUnix)

		sid, err := jwtutil.GenerateSessionID()
		if err != nil {
			return nil, fmt.Errorf("generate sid: %w", err)
		}
		accessToken, err := s.signer.Issue(user.ID, sid, time.Unix(nowUnix, 0), time.Unix(accessExpUnix, 0))
		if err != nil {
			return nil, err
		}
		refreshToken, err := jwtutil.GenerateRefreshToken(sid)
		if err != nil {
			return nil, fmt.Errorf("generate refresh token: %w", err)
		}
		refreshHash := jwtutil.HashToken(refreshToken)

		err = s.sessions.Create(ctx, sid, user.ID, sessionExpUnix, refreshHash, nowUnix)
		if errors.Is(err, repository.ErrSessionExists) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return &model.AuthTokenData{
			AccessToken:  accessToken,
			RefreshToken: refreshToken,
			ExpiresIn:    accessExpUnix - nowUnix,
			User:         user,
		}, nil
	}
	return nil, fmt.Errorf("create auth session: sid collision retries exhausted")
}

func (s *AuthService) avatarURL(key string) string {
	if key == "" {
		key = s.defaultAvatarKey
	}
	return s.storage.URL(key)
}

func validatePasswordForRegister(password string) error {
	if !utf8.ValidString(password) {
		return errno.ErrParam.WithMsg("密码格式无效")
	}
	runes := utf8.RuneCountInString(password)
	if runes < minPasswordRunes || runes > maxPasswordRunes {
		return errno.ErrParam.WithMsg("密码长度需为 8~64 个字符")
	}
	if len(password) > maxPasswordBytes {
		return errno.ErrParam.WithMsg("密码 UTF-8 编码不能超过 72 字节")
	}
	return nil
}

func validateNickname(nickname string) error {
	if !utf8.ValidString(nickname) {
		return errno.ErrParam.WithMsg("昵称格式无效")
	}
	runes := utf8.RuneCountInString(nickname)
	if runes < 1 || runes > maxNicknameRunes {
		return errno.ErrParam.WithMsg("昵称长度需为 1~32 个字符")
	}
	return nil
}
