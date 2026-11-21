// Package response 统一 HTTP 响应格式 {code, msg, data}。
package response

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"wallpaper-feed/internal/pkg/errno"
)

type Body struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data any    `json:"data"`
}

func OK(c *gin.Context, data any) {
	c.JSON(http.StatusOK, Body{Code: 0, Msg: "ok", Data: data})
}

func Created(c *gin.Context, data any) {
	c.JSON(http.StatusCreated, Body{Code: 0, Msg: "ok", Data: data})
}

// Error 把任意错误映射为业务错误码；非 errno.Error 一律视为内部错误并记录日志，不向外暴露细节。
func Error(c *gin.Context, err error) {
	var e *errno.Error
	if !errors.As(err, &e) {
		slog.ErrorContext(c.Request.Context(), "internal error", "path", c.FullPath(), "err", err)
		e = errno.ErrInternal
	}
	c.AbortWithStatusJSON(e.HTTP, Body{Code: e.Code, Msg: e.Message, Data: nil})
}
