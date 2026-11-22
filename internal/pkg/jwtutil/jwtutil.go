package jwtutil

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	TokenTypeBearer    = "Bearer"
	MinSecretLen       = 32
	SessionIDBytes     = 16
	RefreshSecretBytes = 32
	TokenIDBytes       = 16
)

var (
	ErrSecretTooShort      = errors.New("jwtutil: secret must be at least 32 bytes")
	ErrInvalidConfig       = errors.New("jwtutil: invalid manager configuration")
	ErrInvalidToken        = errors.New("jwtutil: invalid access token")
	ErrTokenExpired        = errors.New("jwtutil: access token expired")
	ErrInvalidRefreshToken = errors.New("jwtutil: invalid refresh token format")
)

// AccessClaims 为校验通过后的 Access Token 声明。
type AccessClaims struct {
	UserID    uint64
	SessionID string
	TokenID   string
	Issuer    string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

type rawAccessClaims struct {
	SID string `json:"sid"`
	jwt.RegisteredClaims
}

// Option 用于自定义 Manager 行为（主要用于单元测试注入时钟）。
type Option func(*Manager)

// WithNowFunc 注入自定义当前时间函数。
func WithNowFunc(fn func() time.Time) Option {
	return func(m *Manager) {
		if fn != nil {
			m.nowFunc = fn
		}
	}
}

// Manager 负责 HS256 Access Token 的签发与验签。
type Manager struct {
	secret    []byte
	issuer    string
	accessTTL time.Duration
	nowFunc   func() time.Time
}

// NewManager 构造 JWT 管理器，强制要求签名密钥长度不小于 32 字节。
func NewManager(secret, issuer string, accessTTL time.Duration, opts ...Option) (*Manager, error) {
	if len(secret) < MinSecretLen {
		return nil, ErrSecretTooShort
	}
	if strings.TrimSpace(issuer) == "" || accessTTL <= 0 {
		return nil, ErrInvalidConfig
	}
	m := &Manager{
		secret:    append([]byte(nil), []byte(secret)...),
		issuer:    issuer,
		accessTTL: accessTTL,
		nowFunc:   time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m, nil
}

// GenerateAccessToken 使用默认 accessTTL 签发 HS256 Access Token。
func (m *Manager) GenerateAccessToken(userID uint64, sessionID string) (string, int64, error) {
	return m.GenerateAccessTokenWithMaxExpiry(userID, sessionID, time.Time{})
}

// GenerateAccessTokenWithMaxExpiry 签发 HS256 Access Token，并确保过期时间不超过 maxExpiry。
func (m *Manager) GenerateAccessTokenWithMaxExpiry(userID uint64, sessionID string, maxExpiry time.Time) (string, int64, error) {
	if userID == 0 || !isValidSessionID(sessionID) {
		return "", 0, ErrInvalidToken
	}

	now := m.nowFunc().UTC()
	exp := now.Add(m.accessTTL)
	if !maxExpiry.IsZero() {
		maxUTC := maxExpiry.UTC()
		if exp.After(maxUTC) {
			exp = maxUTC
		}
	}
	if !exp.After(now) {
		return "", 0, ErrTokenExpired
	}

	jti, err := randomBase64URL(TokenIDBytes)
	if err != nil {
		return "", 0, fmt.Errorf("jwtutil: generate jti: %w", err)
	}

	claims := rawAccessClaims{
		SID: sessionID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    m.issuer,
			Subject:   strconv.FormatUint(userID, 10),
			ID:        jti,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(m.secret)
	if err != nil {
		return "", 0, fmt.Errorf("jwtutil: sign token: %w", err)
	}

	expiresIn := max(int64(exp.Sub(now)/time.Second), 1)
	return signed, expiresIn, nil
}

// ParseAccessToken 验证 HS256 签名、签发者、有效期与必要声明。
func (m *Manager) ParseAccessToken(tokenStr string) (*AccessClaims, error) {
	if strings.TrimSpace(tokenStr) == "" {
		return nil, ErrInvalidToken
	}

	var claims rawAccessClaims
	token, err := jwt.ParseWithClaims(
		tokenStr,
		&claims,
		func(t *jwt.Token) (any, error) {
			return m.secret, nil
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(m.issuer),
		jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(m.nowFunc),
	)
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, fmt.Errorf("%w: %v", ErrTokenExpired, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if token == nil || !token.Valid {
		return nil, ErrInvalidToken
	}

	if claims.IssuedAt == nil || claims.ExpiresAt == nil || claims.ID == "" || !isValidSessionID(claims.SID) {
		return nil, ErrInvalidToken
	}
	uid, err := strconv.ParseUint(claims.Subject, 10, 64)
	if err != nil || uid == 0 {
		return nil, ErrInvalidToken
	}

	return &AccessClaims{
		UserID:    uid,
		SessionID: claims.SID,
		TokenID:   claims.ID,
		Issuer:    claims.Issuer,
		IssuedAt:  claims.IssuedAt.Time.UTC(),
		ExpiresAt: claims.ExpiresAt.Time.UTC(),
	}, nil
}

// GenerateSessionID 生成 16 字节随机无填充 Base64URL 会话 ID。
func GenerateSessionID() (string, error) {
	return randomBase64URL(SessionIDBytes)
}

// GenerateRefreshToken 按 <sid>.<secret> 格式生成 Refresh Token（secret 为 32 字节随机数）。
func GenerateRefreshToken(sid string) (string, error) {
	if !isValidSessionID(sid) {
		return "", ErrInvalidRefreshToken
	}
	secret, err := randomBase64URL(RefreshSecretBytes)
	if err != nil {
		return "", fmt.Errorf("jwtutil: generate refresh secret: %w", err)
	}
	return sid + "." + secret, nil
}

// ParseRefreshToken 解析并校验 <sid>.<secret> 格式，防止非法 sid 注入。
func ParseRefreshToken(token string) (sid string, secret string, err error) {
	if token == "" || strings.ContainsAny(token, "\r\n \t=") {
		return "", "", ErrInvalidRefreshToken
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return "", "", ErrInvalidRefreshToken
	}
	if !isValidBase64URLBytes(parts[0], SessionIDBytes) || !isValidBase64URLBytes(parts[1], RefreshSecretBytes) {
		return "", "", ErrInvalidRefreshToken
	}
	return parts[0], parts[1], nil
}

// HashRefreshToken 计算完整 Refresh Token 的 SHA-256 小写十六进制摘要。
func HashRefreshToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// VerifyRefreshTokenHash 使用常量时间比对验证 Refresh Token 摘要。
func VerifyRefreshTokenHash(token, expectedHex string) bool {
	if token == "" || len(expectedHex) != sha256.Size*2 {
		return false
	}
	actual := HashRefreshToken(token)
	return subtle.ConstantTimeCompare([]byte(actual), []byte(expectedHex)) == 1
}

func isValidSessionID(sid string) bool {
	return isValidBase64URLBytes(sid, SessionIDBytes)
}

func isValidBase64URLBytes(s string, expectedBytes int) bool {
	if s == "" || strings.ContainsAny(s, "\r\n \t=") {
		return false
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) != expectedBytes {
		return false
	}
	return base64.RawURLEncoding.EncodeToString(b) == s
}

func randomBase64URL(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
