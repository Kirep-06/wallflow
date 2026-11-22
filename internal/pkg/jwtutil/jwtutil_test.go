package jwtutil

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const testSecret = "0123456789abcdef0123456789abcdef" // 32 bytes

func TestAccessToken(t *testing.T) {
	if _, err := NewManager(strings.Repeat("a", 31), "wallpaper-feed", time.Hour); !errors.Is(err, ErrSecretTooShort) {
		t.Fatalf("expected ErrSecretTooShort, got %v", err)
	}

	now := time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)
	cur := now
	mgr, err := NewManager(testSecret, "wallpaper-feed", 2*time.Hour, WithNowFunc(func() time.Time { return cur }))
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	sid, err := GenerateSessionID()
	if err != nil {
		t.Fatalf("GenerateSessionID failed: %v", err)
	}

	// 签发与验签
	tok, expIn, err := mgr.GenerateAccessToken(10001, sid)
	if err != nil || expIn != 7200 {
		t.Fatalf("GenerateAccessToken failed: expIn=%d err=%v", expIn, err)
	}
	claims, err := mgr.ParseAccessToken(tok)
	if err != nil || claims.UserID != 10001 || claims.SessionID != sid || !claims.ExpiresAt.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("unexpected claims: %+v err=%v", claims, err)
	}

	// 会话剩余时间不足时截断 Access Token 有效期
	clampedTok, expIn, err := mgr.GenerateAccessTokenWithMaxExpiry(10001, sid, now.Add(15*time.Minute))
	if err != nil || expIn != 900 {
		t.Fatalf("clamped token failed: expIn=%d err=%v", expIn, err)
	}
	if c, err := mgr.ParseAccessToken(clampedTok); err != nil || !c.ExpiresAt.Equal(now.Add(15*time.Minute)) {
		t.Fatalf("unexpected clamped exp: %+v err=%v", c, err)
	}

	// 过期拒绝
	cur = now.Add(2*time.Hour + time.Second)
	if _, err := mgr.ParseAccessToken(tok); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expected ErrTokenExpired, got %v", err)
	}
	cur = now

	// 错密钥与非 HS256（alg=none）拒绝
	otherMgr, _ := NewManager("ffffffffffffffffffffffffffffffff", "wallpaper-feed", time.Hour, WithNowFunc(func() time.Time { return cur }))
	if _, err := otherMgr.ParseAccessToken(tok); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for wrong secret, got %v", err)
	}
	noneTok, _ := jwt.NewWithClaims(jwt.SigningMethodNone, rawAccessClaims{
		SID: sid,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "wallpaper-feed",
			Subject:   "10001",
			ID:        "jti-1",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		},
	}).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if _, err := mgr.ParseAccessToken(noneTok); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for alg=none, got %v", err)
	}
}

func TestRefreshToken(t *testing.T) {
	sid, err := GenerateSessionID()
	if err != nil {
		t.Fatalf("GenerateSessionID failed: %v", err)
	}
	rt, err := GenerateRefreshToken(sid)
	if err != nil {
		t.Fatalf("GenerateRefreshToken failed: %v", err)
	}

	parsedSID, secret, err := ParseRefreshToken(rt)
	if err != nil || parsedSID != sid || secret == "" {
		t.Fatalf("ParseRefreshToken failed: sid=%q err=%v", parsedSID, err)
	}

	digest := HashRefreshToken(rt)
	if !VerifyRefreshTokenHash(rt, digest) || VerifyRefreshTokenHash(rt+"x", digest) {
		t.Fatal("VerifyRefreshTokenHash mismatch")
	}

	for _, bad := range []string{"", sid, sid + ".", "short." + secret, "../bad." + secret, sid + "." + secret + ".x"} {
		if _, _, err := ParseRefreshToken(bad); !errors.Is(err, ErrInvalidRefreshToken) {
			t.Fatalf("ParseRefreshToken(%q): expected ErrInvalidRefreshToken, got %v", bad, err)
		}
	}
}
