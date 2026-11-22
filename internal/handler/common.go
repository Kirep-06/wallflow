package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/pkg/errno"
)

const maxJSONBodyBytes = 1 << 20 // 1 MiB

var positiveIntIDPattern = regexp.MustCompile(`^[1-9][0-9]*$`)

// decodeStrictJSON 严格解析 JSON 请求体：限制大小、拒绝空体/顶层 null/未知字段与尾随脏数据。
func decodeStrictJSON(c *gin.Context, dst any) error {
	if c.Request.Body == nil {
		return errno.ErrInvalidParam
	}
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, maxJSONBodyBytes+1))
	if err != nil || len(raw) > maxJSONBodyBytes {
		return errno.ErrInvalidParam
	}

	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return errno.ErrInvalidParam
	}

	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errno.ErrInvalidParam
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errno.ErrInvalidParam
	}
	return nil
}

// parsePositiveUint64 解析路径中的正整数 ID 参数；校验失败返回 10002。
func parsePositiveUint64(raw, errMsg string) (uint64, error) {
	invalidErr := func() error {
		if errMsg != "" {
			return errno.ErrInvalidParam.WithMessage(errMsg)
		}
		return errno.ErrInvalidParam
	}

	if !positiveIntIDPattern.MatchString(raw) {
		return 0, invalidErr()
	}
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		return 0, invalidErr()
	}
	return id, nil
}
