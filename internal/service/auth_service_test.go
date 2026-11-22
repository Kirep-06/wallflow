package service

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestPasswordHelpers(t *testing.T) {
	const raw = "CorrectHorse123!"

	hash, err := hashPassword(raw)
	if err != nil {
		t.Fatalf("hashPassword returned error: %v", err)
	}

	cost, err := bcrypt.Cost([]byte(hash))
	if err != nil {
		t.Fatalf("bcrypt.Cost returned error: %v", err)
	}
	if cost != passwordBcryptCost {
		t.Fatalf("hash cost = %d, want %d", cost, passwordBcryptCost)
	}

	if !verifyPassword(hash, raw) {
		t.Fatal("verifyPassword should match original password")
	}
	if verifyPassword(hash, raw+"x") {
		t.Fatal("verifyPassword should reject mismatched password")
	}
}

func TestDummyPasswordHash(t *testing.T) {
	const goroutines = 8
	results := make([][]byte, goroutines)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			results[idx] = dummyPasswordHash()
		}(i)
	}
	wg.Wait()

	base := results[0]
	cost, err := bcrypt.Cost(base)
	if err != nil {
		t.Fatalf("dummyPasswordHash invalid bcrypt hash: %v", err)
	}
	if cost != passwordBcryptCost {
		t.Fatalf("dummyPasswordHash cost = %d, want %d", cost, passwordBcryptCost)
	}
	for i := 1; i < goroutines; i++ {
		if !bytes.Equal(base, results[i]) {
			t.Fatalf("goroutine %d got different dummyPasswordHash", i)
		}
	}

	// 验证常规密码与超过 72 字节密码均可安全执行防枚举比对而不 panic。
	compareDummyPassword("normal-password")
	compareDummyPassword(strings.Repeat("a", maxPasswordBytes+20))
}

func TestAuthValidators(t *testing.T) {
	t.Run("username", func(t *testing.T) {
		got, err := validateUsername("  alice_01 ")
		if err != nil || got != "alice_01" {
			t.Fatalf("validateUsername valid = (%q, %v), want (\"alice_01\", nil)", got, err)
		}
		for _, bad := range []string{"", "ab", "alice-01", strings.Repeat("a", 33)} {
			if _, err := validateUsername(bad); err == nil {
				t.Fatalf("validateUsername(%q) expected error, got nil", bad)
			}
		}
	})

	t.Run("password", func(t *testing.T) {
		valid := []string{
			"12345678",
			strings.Repeat("a", 64),
			strings.Repeat("密", 24), // 24 runes, 72 UTF-8 bytes
		}
		for _, pwd := range valid {
			if err := validatePassword(pwd); err != nil {
				t.Fatalf("validatePassword(%q) unexpected error: %v", pwd, err)
			}
		}

		invalid := []string{
			"1234567",               // < 8 runes
			strings.Repeat("a", 65), // > 64 runes
			strings.Repeat("密", 25), // 25 runes, 75 UTF-8 bytes (> 72)
		}
		for _, pwd := range invalid {
			if err := validatePassword(pwd); err == nil {
				t.Fatalf("validatePassword(len=%d) expected error, got nil", len(pwd))
			}
		}
	})

	t.Run("register_nickname", func(t *testing.T) {
		got, err := validateRegisterNickname("", "alice_01")
		if err != nil || got != "alice_01" {
			t.Fatalf("empty nickname fallback = (%q, %v), want (\"alice_01\", nil)", got, err)
		}
		got, err = validateRegisterNickname("  小白  ", "alice_01")
		if err != nil || got != "小白" {
			t.Fatalf("trimmed nickname = (%q, %v), want (\"小白\", nil)", got, err)
		}
		if _, err := validateRegisterNickname("   ", "alice_01"); err == nil {
			t.Fatal("whitespace-only nickname expected error, got nil")
		}
		if _, err := validateRegisterNickname(strings.Repeat("昵", maxNicknameRunes+1), "alice_01"); err == nil {
			t.Fatal("oversized nickname expected error, got nil")
		}
	})
}

func TestUserServiceBuildAvatarURL(t *testing.T) {
	svc := NewUserService(nil, nil, "https://cdn.example.com/")
	if got := svc.BuildAvatarURL(""); got != "https://cdn.example.com/avatars/default.png" {
		t.Fatalf("empty avatarKey -> %q", got)
	}
	if got := svc.BuildAvatarURL("/avatars/42/a.png"); got != "https://cdn.example.com/avatars/42/a.png" {
		t.Fatalf("custom avatarKey -> %q", got)
	}

	emptyCDN := NewUserService(nil, nil, "")
	if got := emptyCDN.BuildAvatarURL("avatars/42/a.png"); got != "" {
		t.Fatalf("empty cdnDomain -> %q, want empty", got)
	}
}
