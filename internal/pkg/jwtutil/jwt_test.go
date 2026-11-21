package jwtutil

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const testSecret = "0123456789abcdef0123456789abcdef"

func TestSignerIssueAndParse(t *testing.T) {
	s, err := NewSigner(testSecret, "wallpaper-feed")
	if err != nil {
		t.Fatal(err)
	}
	sid, err := GenerateSessionID()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1763700000, 0)
	exp := now.Add(15 * time.Minute)

	tok, err := s.Issue(42, sid, now, exp)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := s.Parse(tok, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("expected valid token, got err: %v", err)
	}
	if parsed.UserID != 42 || parsed.SID != sid || parsed.JTI == "" || parsed.IssuedAt != now.Unix() || parsed.ExpiresAt != exp.Unix() {
		t.Fatalf("unexpected parsed token: %+v", parsed)
	}
}

func TestSignerRejectsInvalidTokens(t *testing.T) {
	s, _ := NewSigner(testSecret, "wallpaper-feed")
	otherSecret, _ := NewSigner("fedcba9876543210fedcba9876543210", "wallpaper-feed")
	otherIssuer, _ := NewSigner(testSecret, "other-issuer")
	sid, _ := GenerateSessionID()
	now := time.Unix(1763700000, 0)

	validTok, _ := s.Issue(10, sid, now, now.Add(15*time.Minute))

	// 1. 过期（exp == now 与 exp < now 都拒绝）
	if _, err := s.Parse(validTok, now.Add(15*time.Minute)); err == nil {
		t.Fatal("expected expired token at exp == now to fail")
	}
	if _, err := s.Parse(validTok, now.Add(16*time.Minute)); err == nil {
		t.Fatal("expected expired token to fail")
	}

	// 2. iat 在未来
	if _, err := s.Parse(validTok, now.Add(-1*time.Second)); err == nil {
		t.Fatal("expected future iat token to fail")
	}

	// 3. 错误密钥签名
	badSig, _ := otherSecret.Issue(10, sid, now, now.Add(15*time.Minute))
	if _, err := s.Parse(badSig, now); err == nil {
		t.Fatal("expected wrong secret token to fail")
	}

	// 4. 错误 issuer
	badIss, _ := otherIssuer.Issue(10, sid, now, now.Add(15*time.Minute))
	if _, err := s.Parse(badIss, now); err == nil {
		t.Fatal("expected wrong issuer token to fail")
	}

	// 5. 非 HS256 算法（HS512）
	hs512Tok, _ := jwt.NewWithClaims(jwt.SigningMethodHS512, AccessClaims{
		SID: sid,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "wallpaper-feed",
			Subject:   "10",
			ID:        "jti-1",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(10 * time.Minute)),
		},
	}).SignedString([]byte(testSecret))
	if _, err := s.Parse(hs512Tok, now); err == nil {
		t.Fatal("expected HS512 token to fail")
	}

	// 6. 非规范 sub（前导零 / 0）与缺失 jti / 非法 sid
	badCases := []AccessClaims{
		{SID: sid, RegisteredClaims: jwt.RegisteredClaims{Issuer: "wallpaper-feed", Subject: "0", ID: "j1", IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute))}},
		{SID: sid, RegisteredClaims: jwt.RegisteredClaims{Issuer: "wallpaper-feed", Subject: "01", ID: "j1", IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute))}},
		{SID: sid, RegisteredClaims: jwt.RegisteredClaims{Issuer: "wallpaper-feed", Subject: "10", ID: "", IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute))}},
		{SID: "bad-sid", RegisteredClaims: jwt.RegisteredClaims{Issuer: "wallpaper-feed", Subject: "10", ID: "j1", IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute))}},
		{SID: sid, RegisteredClaims: jwt.RegisteredClaims{Issuer: "wallpaper-feed", Subject: "10", ID: "j1", IssuedAt: jwt.NewNumericDate(now.Add(time.Minute)), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute))}},
	}
	for i, c := range badCases {
		raw, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString([]byte(testSecret))
		if _, err := s.Parse(raw, now.Add(10*time.Second)); err == nil {
			t.Fatalf("case %d: expected parse failure", i)
		}
	}
}

func TestRefreshTokenFormatAndHash(t *testing.T) {
	sid, err := GenerateSessionID()
	if err != nil {
		t.Fatal(err)
	}
	rt, err := GenerateRefreshToken(sid)
	if err != nil {
		t.Fatal(err)
	}
	gotSID, ok := ParseRefreshToken(rt)
	if !ok || gotSID != sid {
		t.Fatalf("ParseRefreshToken failed: got (%q, %v), want (%q, true)", gotSID, ok, sid)
	}
	if h := HashToken(rt); len(h) != 64 {
		t.Fatalf("expected 64-char sha256 hex, got %d", len(h))
	}

	// 非法格式：缺段、多余点、带填充 =、字节长度不对
	badBytes := make([]byte, 15)
	_, _ = rand.Read(badBytes)
	shortSID := base64.RawURLEncoding.EncodeToString(badBytes)
	parts := strings.SplitN(rt, ".", 2)
	for _, bad := range []string{
		"",
		sid,
		rt + ".extra",
		parts[0] + "=" + "." + parts[1],
		shortSID + "." + parts[1],
		parts[0] + ".notvalid",
	} {
		if _, ok := ParseRefreshToken(bad); ok {
			t.Fatalf("expected ParseRefreshToken(%q) to fail", bad)
		}
	}
}
