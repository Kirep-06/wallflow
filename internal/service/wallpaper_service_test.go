package service

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
)

func TestNormalizeAndValidateTags(t *testing.T) {
	t.Run("nil or empty returns empty non-nil slice", func(t *testing.T) {
		got, err := normalizeAndValidateTags(nil)
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("normalizeAndValidateTags(nil) = (%v, %v)", got, err)
		}
	})

	t.Run("trim lowercase and deduplicate keeping first order", func(t *testing.T) {
		got, err := normalizeAndValidateTags([]string{" 4K ", "赛博朋克", "4k", "夜景", "赛博朋克"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"4k", "赛博朋克", "夜景"}
		if !slices.Equal(got, want) {
			t.Fatalf("got = %v, want %v", got, want)
		}
	})

	t.Run("exact 24 chinese runes allowed", func(t *testing.T) {
		tag24 := strings.Repeat("画", 24)
		got, err := normalizeAndValidateTags([]string{tag24})
		if err != nil || len(got) != 1 || got[0] != tag24 {
			t.Fatalf("got = (%v, %v)", got, err)
		}
	})

	t.Run("invalid tags return 30003", func(t *testing.T) {
		tests := []struct {
			name string
			tags []string
		}{
			{name: "more than 5 tags even with duplicates", tags: []string{"a", "b", "c", "d", "e", "a"}},
			{name: "blank tag", tags: []string{"   "}},
			{name: "over 24 runes", tags: []string{strings.Repeat("a", 25)}},
			{name: "internal space", tags: []string{"cyber punk"}},
			{name: "internal tab", tags: []string{"cyber\tpunk"}},
			{name: "ascii comma", tags: []string{"a,b"}},
			{name: "fullwidth comma", tags: []string{"动漫，夜景"}},
			{name: "control char", tags: []string{"abc\x00def"}},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := normalizeAndValidateTags(tc.tags); !errors.Is(err, errno.ErrInvalidCategoryOrTag) {
					t.Fatalf("err = %v, want ErrInvalidCategoryOrTag", err)
				}
			})
		}
	})
}

func TestEscapeLikePrefix(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "cyber", want: "cyber%"},
		{in: "100%_a\\b", want: `100\%\_a\\b%`},
		{in: `\\%_`, want: `\\\\\%\_%`},
	}

	for _, tc := range tests {
		if got := escapeLikePrefix(tc.in); got != tc.want {
			t.Fatalf("escapeLikePrefix(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseCategoryID(t *testing.T) {
	validTests := []struct {
		in   string
		want uint64
	}{
		{in: "1", want: 1},
		{in: "10001", want: 10001},
	}
	for _, tc := range validTests {
		got, err := parseCategoryID(tc.in)
		if err != nil || got != tc.want {
			t.Fatalf("parseCategoryID(%q) = (%d, %v), want (%d, nil)", tc.in, got, err, tc.want)
		}
	}

	invalidInputs := []string{"", "0", "01", "-1", " 1 ", "1a", "999999999999999999999"}
	for _, in := range invalidInputs {
		if _, err := parseCategoryID(in); !errors.Is(err, errno.ErrInvalidParam) {
			t.Fatalf("parseCategoryID(%q) err = %v, want ErrInvalidParam", in, err)
		}
	}
}

func TestWallpaperServicePreDBValidation(t *testing.T) {
	svc := NewWallpaperService(nil, nil, nil, &stubStorage{})
	validKey := "wallpapers/10/20261122/9f3c2e8a4b1d4a7c8e5f1a2b3c4d5e6f.webp"

	tests := []struct {
		name    string
		req     model.CreateWallpaperRequest
		wantErr error
	}{
		{
			name:    "empty title",
			req:     model.CreateWallpaperRequest{Title: "   ", CategoryID: "1", ObjectKey: validKey},
			wantErr: errno.ErrInvalidParam,
		},
		{
			name:    "title over 80 runes",
			req:     model.CreateWallpaperRequest{Title: strings.Repeat("星", 81), CategoryID: "1", ObjectKey: validKey},
			wantErr: errno.ErrInvalidParam,
		},
		{
			name:    "description over 500 runes",
			req:     model.CreateWallpaperRequest{Title: "ok", Description: strings.Repeat("字", 501), CategoryID: "1", ObjectKey: validKey},
			wantErr: errno.ErrInvalidParam,
		},
		{
			name:    "invalid category_id format",
			req:     model.CreateWallpaperRequest{Title: "ok", CategoryID: "01", ObjectKey: validKey},
			wantErr: errno.ErrInvalidParam,
		},
		{
			name:    "invalid tags before db",
			req:     model.CreateWallpaperRequest{Title: "ok", CategoryID: "1", Tags: []string{"bad tag"}, ObjectKey: validKey},
			wantErr: errno.ErrInvalidCategoryOrTag,
		},
		{
			name:    "foreign object_key before db",
			req:     model.CreateWallpaperRequest{Title: "ok", CategoryID: "1", ObjectKey: "wallpapers/99/20261122/9f3c2e8a4b1d4a7c8e5f1a2b3c4d5e6f.webp"},
			wantErr: errno.ErrInvalidParam,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := svc.CreateWallpaper(context.Background(), 10, tc.req); !errors.Is(err, tc.wantErr) {
				t.Fatalf("CreateWallpaper err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
