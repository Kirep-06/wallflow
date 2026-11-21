// Package cursor 实现不透明游标的编解码：Base64URL(JSON)。
package cursor

import (
	"encoding/base64"
	"encoding/json"
	"errors"
)

var ErrInvalid = errors.New("invalid cursor")

// IDCursor 是按 ID 倒序分页的游标。
type IDCursor struct {
	ID uint64 `json:"id"`
}

// EncodeID 编码 ID 游标；id 为 0 时返回空串（表示没有下一页）。
func EncodeID(id uint64) string {
	if id == 0 {
		return ""
	}
	b, _ := json.Marshal(IDCursor{ID: id})
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeID 解码 ID 游标；空串返回 0（表示从头开始）。
func DecodeID(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return 0, ErrInvalid
	}
	var c IDCursor
	if err := json.Unmarshal(b, &c); err != nil || c.ID == 0 {
		return 0, ErrInvalid
	}
	return c.ID, nil
}
