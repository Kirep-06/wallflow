package cursor

import (
	"encoding/base64"
	"errors"
	"math"
	"strings"
	"testing"

	"wallpaper-feed/internal/pkg/errno"
)

func TestEncodeDecode(t *testing.T) {
	token, err := Encode(IDCursor{ID: 1001})
	if err != nil || token != "eyJpZCI6MTAwMX0" {
		t.Fatalf("Encode(1001) = %q, err=%v", token, err)
	}
	if got, err := Decode[IDCursor](token); err != nil || got.ID != 1001 {
		t.Fatalf("Decode(1001) = %+v, err=%v", got, err)
	}

	// 覆盖 uint64 最大值与复合游标。
	big := TimeCursor{Time: 1795248000000, ID: math.MaxUint64}
	bigTok, err := Encode(big)
	if err != nil {
		t.Fatalf("Encode TimeCursor failed: %v", err)
	}
	if got, err := Decode[TimeCursor](bigTok); err != nil || got != big {
		t.Fatalf("Decode TimeCursor = %+v, want %+v, err=%v", got, big, err)
	}
}

func TestDecode_Invalid(t *testing.T) {
	b64 := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	invalid := []string{
		"",
		"eyJpZCI6MX0=",           // 带 padding
		"!!!bad!!!",              // 非法 base64
		b64("not-json"),          // 非 JSON
		b64("null"),              // JSON null
		b64(`{"id":1,"x":2}`),    // 未知字段
		b64(`{"id":1}{"id":2}`),  // 尾随脏数据
		strings.Repeat("a", 1025), // 超长
	}

	for _, tok := range invalid {
		if _, err := Decode[IDCursor](tok); !errors.Is(err, errno.ErrInvalidCursor) {
			t.Fatalf("Decode(%q): expected ErrInvalidCursor, got %v", tok, err)
		}
	}
}

func TestParseLimit(t *testing.T) {
	cases := []struct {
		raw     string
		want    int
		wantErr bool
	}{
		{"", DefaultLimit, false},
		{"1", 1, false},
		{"50", 50, false},
		{"0", 0, true},
		{"51", 0, true},
		{"abc", 0, true},
	}

	for _, tc := range cases {
		got, err := ParseLimit(tc.raw, DefaultLimit, MaxLimit)
		if tc.wantErr {
			if !errors.Is(err, errno.ErrInvalidParam) {
				t.Fatalf("raw=%q: expected ErrInvalidParam, got %v", tc.raw, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Fatalf("raw=%q: got=%d err=%v, want=%d", tc.raw, got, err, tc.want)
		}
	}
}
