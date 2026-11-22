package cursor

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"wallpaper-feed/internal/pkg/errno"
)

const (
	MaxCursorLen = 1024
	DefaultLimit = 20
	MinLimit     = 1
	MaxLimit     = 50
)

// ErrInvalid 表示分页游标无效（对应业务码 50001）。
var ErrInvalid = errno.ErrInvalidCursor

// IDCursor 为单主键降序/升序游标载荷。
type IDCursor struct {
	ID uint64 `json:"id"`
}

// TimeCursor 为时间与主键复合游标载荷。
type TimeCursor struct {
	Time int64  `json:"t"`
	ID   uint64 `json:"id"`
}

// ScoreCursor 为分值与主键复合游标载荷（用于热榜等场景）。
type ScoreCursor struct {
	Score float64 `json:"s"`
	ID    uint64  `json:"id"`
}

// Encode 将游标结构体编码为无填充 Base64URL 字符串。
func Encode[T any](payload T) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("cursor encode marshal: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// Decode 将无填充 Base64URL 游标解码为目标结构体；任何格式非法均返回 ErrInvalid（50001）。
func Decode[T any](token string) (T, error) {
	var zero T
	if token == "" || len(token) > MaxCursorLen {
		return zero, ErrInvalid.Wrap(errors.New("cursor length out of range"))
	}
	if strings.ContainsAny(token, "\r\n \t=") {
		return zero, ErrInvalid.Wrap(errors.New("cursor contains invalid characters"))
	}

	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return zero, ErrInvalid.Wrap(err)
	}
	if base64.RawURLEncoding.EncodeToString(raw) != token {
		return zero, ErrInvalid.Wrap(errors.New("cursor is not canonical base64url"))
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return zero, ErrInvalid.Wrap(errors.New("cursor payload is null or empty"))
	}

	var out T
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return zero, ErrInvalid.Wrap(err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return zero, ErrInvalid.Wrap(errors.New("cursor has trailing data"))
	}
	return out, nil
}

// ParseLimit 解析分页 limit 查询参数：空串返回 defaultLimit，范围限制为 [1, maxLimit]。
func ParseLimit(raw string, defaultLimit, maxLimit int) (int, error) {
	if defaultLimit <= 0 {
		defaultLimit = DefaultLimit
	}
	if maxLimit <= 0 {
		maxLimit = MaxLimit
	}
	if strings.TrimSpace(raw) == "" {
		return defaultLimit, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < MinLimit || n > maxLimit {
		return 0, errno.ErrInvalidParam.WithMessage("limit 参数不合法")
	}
	return n, nil
}
