// Package jwtutil 提供 HS256 Access Token 签发/校验以及 Refresh Token 格式与摘要工具。
package jwtutil

import (
	"crypto/rand"
	"crypto/sha256"
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
	sessionIDBytes     = 16
	refreshSecretBytes = 32
	jtiBytes           = 16
)

var ErrInvalidToken = errors.New("invalid token")

type AccessClaims struct {
	SID string `json:"sid"`
	jwt.RegisteredClaims
}

type ParsedAccessToken struct {
	UserID    uint64
	SID       string
	JTI       string
	IssuedAt  int64
	ExpiresAt int64
}

type Signer struct {
	secret []byte
	issuer string
}

func NewSigner(secret, issuer string) (*Signer, error) {
	if len(secret) < 32 {
		return nil, fmt.Errorf("jwt secret must be at least 32 bytes")
	}
	if strings.TrimSpace(issuer) == "" {
		return nil, fmt.Errorf("jwt issuer must not be empty")
	}
	return &Signer{
		secret: []byte(secret),
		issuer: issuer,
	}, nil
}

// Issue 签发 HS256 Access Token；要求 userID > 0、sid 合法且 iat < exp。
func (s *Signer) Issue(userID uint64, sid string, iat, exp time.Time) (string, error) {
	if userID == 0 || !ValidateSessionID(sid) {
		return "", fmt.Errorf("invalid token subject or sid")
	}
	iatUnix := iat.Unix()
	expUnix := exp.Unix()
	if iatUnix >= expUnix {
		return "", fmt.Errorf("invalid token lifetime: iat >= exp")
	}
	jti, err := randomHex(jtiBytes)
	if err != nil {
		return "", fmt.Errorf("generate jti: %w", err)
	}
	claims := AccessClaims{
		SID: sid,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   strconv.FormatUint(userID, 10),
			ID:        jti,
			IssuedAt:  jwt.NewNumericDate(time.Unix(iatUnix, 0)),
			ExpiresAt: jwt.NewNumericDate(time.Unix(expUnix, 0)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(s.secret)
	if err != nil {
		return "", fmt.Errorf("sign access token: %w", err)
	}
	return signed, nil
}

// Parse 严格校验算法、签名、iss、exp、iat、sub、jti 与 sid。
func (s *Signer) Parse(raw string, now time.Time) (*ParsedAccessToken, error) {
	if raw == "" {
		return nil, ErrInvalidToken
	}
	nowUnix := now.Unix()
	var claims AccessClaims
	tok, err := jwt.ParseWithClaims(
		raw,
		&claims,
		func(t *jwt.Token) (any, error) {
			if t.Method == nil || t.Method.Alg() != jwt.SigningMethodHS256.Alg() {
				return nil, ErrInvalidToken
			}
			return s.secret, nil
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(s.issuer),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithTimeFunc(func() time.Time { return time.Unix(nowUnix, 0) }),
	)
	if err != nil || tok == nil || !tok.Valid {
		return nil, ErrInvalidToken
	}
	if claims.Issuer != s.issuer || claims.IssuedAt == nil || claims.ExpiresAt == nil {
		return nil, ErrInvalidToken
	}
	iatUnix := claims.IssuedAt.Unix()
	expUnix := claims.ExpiresAt.Unix()
	if iatUnix >= expUnix || iatUnix > nowUnix || expUnix <= nowUnix {
		return nil, ErrInvalidToken
	}
	if claims.ID == "" || !ValidateSessionID(claims.SID) {
		return nil, ErrInvalidToken
	}
	uid, err := strconv.ParseUint(claims.Subject, 10, 64)
	if err != nil || uid == 0 || strconv.FormatUint(uid, 10) != claims.Subject {
		return nil, ErrInvalidToken
	}
	return &ParsedAccessToken{
		UserID:    uid,
		SID:       claims.SID,
		JTI:       claims.ID,
		IssuedAt:  iatUnix,
		ExpiresAt: expUnix,
	}, nil
}

// GenerateSessionID 生成 16 字节随机数的无填充 Base64URL 会话标识。
func GenerateSessionID() (string, error) {
	return randomBase64URL(sessionIDBytes)
}

// GenerateRefreshToken 按 <sid>.<secret> 构造 Refresh Token，其中 secret 为 32 字节随机数。
func GenerateRefreshToken(sid string) (string, error) {
	if !ValidateSessionID(sid) {
		return "", fmt.Errorf("invalid sid")
	}
	secret, err := randomBase64URL(refreshSecretBytes)
	if err != nil {
		return "", err
	}
	return sid + "." + secret, nil
}

// ParseRefreshToken 校验 <sid>.<secret> 格式并提取 sid。
func ParseRefreshToken(raw string) (sid string, ok bool) {
	if strings.Count(raw, ".") != 1 {
		return "", false
	}
	parts := strings.SplitN(raw, ".", 2)
	if !ValidateSessionID(parts[0]) || !validateRawBase64URL(parts[1], refreshSecretBytes) {
		return "", false
	}
	return parts[0], true
}

// ValidateSessionID 检查 sid 是否为 16 字节规范无填充 Base64URL。
func ValidateSessionID(sid string) bool {
	return validateRawBase64URL(sid, sessionIDBytes)
}

// HashToken 返回完整凭证的 SHA-256 小写十六进制摘要。
func HashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func validateRawBase64URL(s string, expectedBytes int) bool {
	if s == "" {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(decoded) != expectedBytes {
		return false
	}
	return base64.RawURLEncoding.EncodeToString(decoded) == s
}

func randomBase64URL(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
