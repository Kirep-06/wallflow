package model

import (
	"bytes"
	"encoding/json"
)

// PatchString 用于 PATCH 请求中精确区分“未传字段”、“显式传 null”与“正常字符串值”。
type PatchString struct {
	Set   bool
	Null  bool
	Value string
}

// UnmarshalJSON 解析 JSON 字符串字段，显式标记 null。
func (p *PatchString) UnmarshalJSON(data []byte) error {
	p.Set = true
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		p.Null = true
		return nil
	}
	return json.Unmarshal(data, &p.Value)
}
