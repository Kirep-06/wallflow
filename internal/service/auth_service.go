package service

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/bcrypt"

	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/jwtutil"
	"wallpaper-feed/internal/repository"
)

const (
	sessionTTL         = 7 * 24 * time.Hour
	passwordBcryptCost = bcrypt.DefaultCost
	minPasswordRunes   = 8
	maxPasswordRunes   = 64
	maxPasswordBytes   = 72
)

var (
	usernamePattern = regexp.MustCompile(`^[a-zA-Z0-9_]{3,32}$`)

	// dummyPasswordHash 按与真实密码一致的 bcrypt 成本懒生成，用于防用户名枚举时序攻击。
	dummyPasswordHash = sync.OnceValue(func() []byte {
		h, err := bcrypt.GenerateFromPassword([]byte("wf-anti-enumeration-dummy-secret"), passwordBcryptCost)
		if err != nil {
			panic("service: init dummy password hash: " + err.Error())
		}
		return h
	})
)

// AuthenticatedUser 为鉴权中间件校验通过后写入上下文的用户身份。
type AuthenticatedUser struct {
	UserID    uint64
	SessionID string
}

// AuthService 负责注册、登录、刷新令牌、登出与请求鉴权逻辑。
type AuthService struct {
	userRepo    *repository.UserRepository
	sessionRepo *repository.AuthSessionRepository
	userSvc     *UserService
	jwtManager  *jwtutil.Manager
}

// NewAuthService 构造认证领域服务。
func NewAuthService(
	userRepo *repository.UserRepository,
	sessionRepo *repository.AuthSessionRepository,
	userSvc *UserService,
	jwtManager *jwtutil.Manager,
) *AuthService {
	return &AuthService{
		userRepo:    userRepo,
		sessionRepo: sessionRepo,
		userSvc:     userSvc,
		jwtManager:  jwtManager,
	}
}

// Register 注册新用户并签发初始登录会话。
func (s *AuthService) Register(ctx context.Context, req model.RegisterRequest) (*model.AuthResult, error) {
	username, err := validateUsername(req.Username)
	if err != nil {
		return nil, err
	}
	if err := validatePassword(req.Password); err != nil {
		return nil, err
	}
	nickname, err := validateRegisterNickname(req.Nickname, username)
	if err != nil {
		return nil, err
	}

	passwordHash, err := hashPassword(req.Password)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	user := &model.User{
		Username:     username,
		PasswordHash: passwordHash,
		Nickname:     nickname,
	}
	if err := s.userRepo.Create(ctx, user); err != nil {
		if errors.Is(err, repository.ErrDuplicateUsername) {
			return nil, errno.ErrUsernameExists
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	// 清除可能命中的旧空值缓存标记。
	s.userSvc.InvalidateProfileCache(ctx, user.ID)

	pair, err := s.createSessionAndTokens(ctx, user.ID)
	if err != nil {
		return nil, err
	}

	return &model.AuthResult{
		TokenPair: pair,
		User:      s.userSvc.ToUserProfile(user.ToProfileCache(), 0),
	}, nil
}

// Login 校验用户名与密码并创建登录会话；用户不存在时执行等成本 bcrypt 比对防枚举。
func (s *AuthService) Login(ctx context.Context, req model.LoginRequest) (*model.AuthResult, error) {
	username := strings.TrimSpace(req.Username)
	if username == "" || req.Password == "" {
		return nil, errno.ErrInvalidParam.WithMessage("用户名和密码不能为空")
	}
	if len(req.Password) > maxPasswordBytes {
		compareDummyPassword(req.Password)
		return nil, errno.ErrInvalidCredentials
	}

	rec, err := s.userRepo.FindAuthByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			compareDummyPassword(req.Password)
			return nil, errno.ErrInvalidCredentials
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	if !verifyPassword(rec.PasswordHash, req.Password) {
		return nil, errno.ErrInvalidCredentials
	}

	pair, err := s.createSessionAndTokens(ctx, rec.ID)
	if err != nil {
		return nil, err
	}

	profile, err := s.userSvc.BuildUserProfile(ctx, rec.ToProfileCache())
	if err != nil {
		return nil, err
	}

	return &model.AuthResult{
		TokenPair: pair,
		User:      profile,
	}, nil
}

// Refresh 校验刷新令牌并签发新双 Token，严格保持原会话绝对到期时间不无限续命。
func (s *AuthService) Refresh(ctx context.Context, req model.RefreshRequest) (*model.TokenPair, error) {
	rawToken := strings.TrimSpace(req.RefreshToken)
	if rawToken == "" {
		return nil, errno.ErrInvalidParam.WithMessage("refresh_token 不能为空")
	}

	sid, _, err := jwtutil.ParseRefreshToken(rawToken)
	if err != nil {
		return nil, errno.ErrInvalidRefreshToken
	}

	session, err := s.sessionRepo.Get(ctx, sid)
	if err != nil {
		if errors.Is(err, repository.ErrSessionNotFound) {
			return nil, errno.ErrInvalidRefreshToken
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	now := time.Now().UTC()
	if !session.ExpiresAt.After(now) || !jwtutil.VerifyRefreshTokenHash(rawToken, session.RefreshSHA256) {
		return nil, errno.ErrInvalidRefreshToken
	}

	exists, err := s.userRepo.ExistsByID(ctx, session.UserID)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}
	if !exists {
		s.deleteSessionBestEffort(ctx, sid)
		return nil, errno.ErrInvalidRefreshToken
	}

	pair, newHash, err := s.issueTokenPair(session.UserID, sid, session.ExpiresAt)
	if err != nil {
		if errors.Is(err, jwtutil.ErrTokenExpired) {
			return nil, errno.ErrInvalidRefreshToken
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	if err := s.sessionRepo.UpdateRefreshHash(ctx, sid, session.RefreshSHA256, newHash); err != nil {
		if errors.Is(err, repository.ErrSessionNotFound) || errors.Is(err, repository.ErrSessionRefreshMismatch) {
			return nil, errno.ErrInvalidRefreshToken
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	return &pair, nil
}

// Logout 删除当前 Redis 会话，使同会话 Access 与 Refresh Token 立即全部失效。
func (s *AuthService) Logout(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return errno.ErrUnauthorized
	}
	if err := s.sessionRepo.Delete(ctx, sessionID); err != nil {
		return errno.ErrInternal.Wrap(err)
	}
	return nil
}

// Authenticate 验证 Access Token 签名与 Redis 会话；Redis 故障严格 fail-closed 返回 10001。
func (s *AuthService) Authenticate(ctx context.Context, accessToken string) (*AuthenticatedUser, error) {
	claims, err := s.jwtManager.ParseAccessToken(accessToken)
	if err != nil {
		return nil, errno.ErrUnauthorized
	}

	session, err := s.sessionRepo.Get(ctx, claims.SessionID)
	if err != nil {
		if errors.Is(err, repository.ErrSessionNotFound) {
			return nil, errno.ErrUnauthorized
		}
		// Redis 不可用时必须 fail-closed 返回 10001，禁止仅凭 JWT 放行。
		return nil, errno.ErrInternal.Wrap(err)
	}

	if session.UserID != claims.UserID || !session.ExpiresAt.After(time.Now().UTC()) {
		return nil, errno.ErrUnauthorized
	}

	return &AuthenticatedUser{
		UserID:    claims.UserID,
		SessionID: claims.SessionID,
	}, nil
}

func (s *AuthService) createSessionAndTokens(ctx context.Context, userID uint64) (model.TokenPair, error) {
	sid, err := jwtutil.GenerateSessionID()
	if err != nil {
		return model.TokenPair{}, errno.ErrInternal.Wrap(err)
	}

	expiresAt := time.Now().UTC().Add(sessionTTL)
	pair, refreshHash, err := s.issueTokenPair(userID, sid, expiresAt)
	if err != nil {
		return model.TokenPair{}, errno.ErrInternal.Wrap(err)
	}

	session := repository.AuthSession{
		SessionID:     sid,
		UserID:        userID,
		RefreshSHA256: refreshHash,
		ExpiresAt:     expiresAt,
	}
	if err := s.sessionRepo.Create(ctx, session); err != nil {
		return model.TokenPair{}, errno.ErrInternal.Wrap(err)
	}
	return pair, nil
}

func (s *AuthService) issueTokenPair(userID uint64, sid string, maxExpiry time.Time) (model.TokenPair, string, error) {
	accessToken, expiresIn, err := s.jwtManager.GenerateAccessTokenWithMaxExpiry(userID, sid, maxExpiry)
	if err != nil {
		return model.TokenPair{}, "", err
	}

	refreshToken, err := jwtutil.GenerateRefreshToken(sid)
	if err != nil {
		return model.TokenPair{}, "", err
	}

	return model.TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		TokenType:    jwtutil.TokenTypeBearer,
		ExpiresIn:    expiresIn,
	}, jwtutil.HashRefreshToken(refreshToken), nil
}

func (s *AuthService) deleteSessionBestEffort(ctx context.Context, sid string) {
	if err := s.sessionRepo.Delete(ctx, sid); err != nil {
		slog.WarnContext(ctx, "delete auth session failed",
			slog.String("sid", sid),
			slog.Any("err", err),
		)
	}
}

func hashPassword(password string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(password), passwordBcryptCost)
	if err != nil {
		return "", err
	}
	return string(h), nil
}

func verifyPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

func compareDummyPassword(password string) {
	input := []byte(password)
	if len(input) > maxPasswordBytes {
		input = []byte("invalid-len")
	}
	_ = bcrypt.CompareHashAndPassword(dummyPasswordHash(), input)
}

func validateUsername(raw string) (string, error) {
	u := strings.TrimSpace(raw)
	if !usernamePattern.MatchString(u) {
		return "", errno.ErrInvalidParam.WithMessage("用户名需为 3~32 位字母、数字或下划线")
	}
	return u, nil
}

func validatePassword(pwd string) error {
	runes := utf8.RuneCountInString(pwd)
	if runes < minPasswordRunes || runes > maxPasswordRunes || len(pwd) > maxPasswordBytes {
		return errno.ErrInvalidParam.WithMessage("密码需为 8~64 个字符且 UTF-8 长度不超过 72 字节")
	}
	return nil
}

func validateRegisterNickname(raw, fallbackUsername string) (string, error) {
	if raw == "" {
		return fallbackUsername, nil
	}
	return validateNickname(raw)
}
