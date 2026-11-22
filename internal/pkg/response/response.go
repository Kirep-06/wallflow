package response

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/pkg/errno"
)

// Envelope 为所有 HTTP 业务接口统一响应信封。
type Envelope struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data any    `json:"data"`
}

// PageData 为游标分页通用数据结构（供 M3+ 列表接口复用）。
type PageData[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
}

// NewPageData 构造分页响应，保证空列表序列化为 [] 而非 null。
func NewPageData[T any](items []T, nextCursor string, hasMore bool) PageData[T] {
	if items == nil {
		items = make([]T, 0)
	}
	return PageData[T]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}
}

// OK 返回 200 成功信封。
func OK(c *gin.Context, data any) {
	c.JSON(http.StatusOK, Envelope{
		Code: errno.CodeOK,
		Msg:  "ok",
		Data: data,
	})
}

// Created 返回 201 创建成功信封。
func Created(c *gin.Context, data any) {
	c.JSON(http.StatusCreated, Envelope{
		Code: errno.CodeOK,
		Msg:  "ok",
		Data: data,
	})
}

// Fail 将错误映射为统一失败信封；5xx 错误自动记录服务端日志且不泄露底层详情。
func Fail(c *gin.Context, err error) {
	bizErr := errno.FromError(err)
	if bizErr == nil {
		bizErr = errno.ErrInternal
	}
	if bizErr.HTTPStatus >= http.StatusInternalServerError {
		slog.ErrorContext(c.Request.Context(), "http request internal error",
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.Int("code", bizErr.Code),
			slog.Any("err", err),
		)
	}
	c.JSON(bizErr.HTTPStatus, Envelope{
		Code: bizErr.Code,
		Msg:  bizErr.Msg,
		Data: nil,
	})
}

// AbortWithError 在中间件中写入失败信封并终止后续处理链。
func AbortWithError(c *gin.Context, err error) {
	Fail(c, err)
	c.Abort()
}
