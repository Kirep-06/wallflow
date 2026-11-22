package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/bcrypt"

	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/pkg/jwtutil"
	"wallpaper-feed/internal/repository"
)

const (
	minPasswordRunes = 8
	maxPasswordRunes = 64
	maxPasswordBytes = 72
)

var usernamePattern = regexp.MustCompile(`^[a-zA-Z0-9_]{3,32}$`)

// AuthenticatedUser 为鉴权中间件校验通过后写入上下文的用户身份。
type AuthenticatedUser struct {
	UserID    uint64
	SessionID string
}

// AuthService 负责注册、登录、刷新令牌、登出与请求鉴权逻辑。
type AuthService struct {
	userRepo    repository.UserRepository
	sessionRepo repository.AuthSessionRepository
	userSvc     *UserService
	jwtManager  *jwtutil.Manager
	refreshTTL  time.Duration
	bcryptCost  int
	dummyHash   []byte
	nowFunc     func() time.Time
	logger      *slog.Logger
}

// NewAuthService 构造认证领域服务，并预生成同成本 dummyHash 用于防用户名枚举。
func NewAuthService(
	userRepo repository.UserRepository,
	sessionRepo repository.AuthSessionRepository,
	userSvc *UserService,
	jwtManager *jwtutil.Manager,
	refreshTTL time.Duration,
	bcryptCost int,
	logger *slog.Logger,
) (*AuthService, error) {
	if bcryptCost == 0 {
		bcryptCost = bcrypt.DefaultCost
	}
	dummyHash, err := bcrypt.GenerateFromPassword([]byte("wf-anti-enumeration-dummy-secret"), bcryptCost)
	if err != nil {
		return nil, fmt.Errorf("auth service init dummy hash: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &AuthService{
		userRepo:    userRepo,
		sessionRepo: sessionRepo,
		userSvc:     userSvc,
		jwtManager:  jwtManager,
		refreshTTL:  refreshTTL,
		bcryptCost:  bcryptCost,
		dummyHash:   dummyHash,
		nowFunc:     time.Now,
		logger:      logger,
	}, nil
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

	hashBytes, err := bcrypt.GenerateFromPassword([]byte(req.Password), s.bcryptCost)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	user := &model.User{
		Username:     username,
		PasswordHash: string(hashBytes),
		Nickname:     nickname,
		Status:       model.UserStatusActive,
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
		User:      s.userSvc.ToUserProfile(user.ToProfileCache()),
	}, nil
}

// Login 校验用户名与密码并创建登录会话；用户不存在时执行等成本 bcrypt 比对防枚举。
func (s *AuthService) Login(ctx context.Context, req model.LoginRequest) (*model.AuthResult, error) {
	username := strings.TrimSpace(req.Username)
	if username == "" || req.Password == "" {
		return nil, errno.ErrInvalidParam.WithMessage("用户名和密码不能为空")
	}
	if len(req.Password) > maxPasswordBytes {
		_ = bcrypt.CompareHashAndPassword(s.dummyHash, []byte("invalid-len"))
		return nil, errno.ErrInvalidCredentials
	}

	rec, err := s.userRepo.FindAuthByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			_ = bcrypt.CompareHashAndPassword(s.dummyHash, []byte(req.Password))
			return nil, errno.ErrInvalidCredentials
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	if err := bcrypt.CompareHashAndPassword([]byte(rec.PasswordHash), []byte(req.Password)); err != nil {
		return nil, errno.ErrInvalidCredentials
	}

	if rec.Status != model.UserStatusActive {
		return nil, errno.ErrAccountBanned
	}

	pair, err := s.createSessionAndTokens(ctx, rec.ID)
	if err != nil {
		return nil, err
	}

	return &model.AuthResult{
		TokenPair: pair,
		User:      s.userSvc.ToUserProfile(rec.ToProfileCache()),
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

	now := s.nowFunc().UTC()
	if !session.ExpiresAt.After(now) || !jwtutil.VerifyRefreshTokenHash(rawToken, session.RefreshSHA256) {
		return nil, errno.ErrInvalidRefreshToken
	}

	status, err := s.userRepo.GetStatusByID(ctx, session.UserID)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			s.deleteSessionBestEffort(ctx, sid)
			return nil, errno.ErrInvalidRefreshToken
		}
		return nil, errno.ErrInternal.Wrap(err)
	}
	if status != model.UserStatusActive {
		s.deleteSessionBestEffort(ctx, sid)
		return nil, errno.ErrAccountBanned
	}

	accessToken, expiresIn, err := s.jwtManager.GenerateAccessTokenWithMaxExpiry(session.UserID, sid, session.ExpiresAt)
	if err != nil {
		if errors.Is(err, jwtutil.ErrTokenExpired) {
			return nil, errno.ErrInvalidRefreshToken
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	newRefreshToken, err := jwtutil.GenerateRefreshToken(sid)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}
	newHash := jwtutil.HashRefreshToken(newRefreshToken)

	if err := s.sessionRepo.UpdateRefreshHash(ctx, sid, session.RefreshSHA256, newHash); err != nil {
		if errors.Is(err, repository.ErrSessionNotFound) || errors.Is(err, repository.ErrSessionRefreshMismatch) {
			return nil, errno.ErrInvalidRefreshToken
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	return &model.TokenPair{
		AccessToken:  accessToken,
		RefreshToken: newRefreshToken,
		TokenType:    jwtutil.TokenTypeBearer,
		ExpiresIn:    expiresIn,
	}, nil
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

	uid, expiresAt, err := s.sessionRepo.GetAuthCheck(ctx, claims.SessionID)
	if err != nil {
		if errors.Is(err, repository.ErrSessionNotFound) {
			return nil, errno.ErrUnauthorized
		}
		// Redis 不可用时必须 fail-closed 返回 10001，禁止仅凭 JWT 放行。
		return nil, errno.ErrInternal.Wrap(err)
	}

	if uid != claims.UserID || !expiresAt.After(s.nowFunc().UTC()) {
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

	refreshToken, err := jwtutil.GenerateRefreshToken(sid)
	if err != nil {
		return model.TokenPair{}, errno.ErrInternal.Wrap(err)
	}

	expiresAt := s.nowFunc().UTC().Add(s.refreshTTL)
	accessToken, expiresIn, err := s.jwtManager.GenerateAccessTokenWithMaxExpiry(userID, sid, expiresAt)
	if err != nil {
		return model.TokenPair{}, errno.ErrInternal.Wrap(err)
	}

	session := repository.AuthSession{
		SessionID:     sid,
		UserID:        userID,
		RefreshSHA256: jwtutil.HashRefreshToken(refreshToken),
		ExpiresAt:     expiresAt,
	}
	if err := s.sessionRepo.Create(ctx, session); err != nil {
		return model.TokenPair{}, errno.ErrInternal.Wrap(err)
	}

	return model.TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		TokenType:    jwtutil.TokenTypeBearer,
		ExpiresIn:    expiresIn,
	}, nil
}

func (s *AuthService) deleteSessionBestEffort(ctx context.Context, sid string) {
	if err := s.sessionRepo.Delete(ctx, sid); err != nil {
		s.logger.WarnContext(ctx, "delete auth session failed",
			slog.String("sid", sid),
			slog.Any("err", err),
		)
	}
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
	nick := strings.TrimSpace(raw)
	n := utf8.RuneCountInString(nick)
	if n < 1 || n > maxNicknameRunes {
		return "", errno.ErrInvalidParam.WithMessage("昵称长度需为 1~32 个字符")
	}
	return nick, nil
}
