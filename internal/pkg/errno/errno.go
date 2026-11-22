package errno

import (
	"errors"
	"fmt"
	"net/http"
)

const CodeOK = 0

// Error 表示携带业务码、HTTP 状态码与对外安全文案的业务错误。
// 内部原始错误仅存于 err 字段，用于日志记录，不暴露给客户端。
type Error struct {
	Code       int
	HTTPStatus int
	Msg        string
	err        error
}

// 1xxxx：通用与鉴权错误码。
var (
	ErrInternal        = &Error{Code: 10001, HTTPStatus: http.StatusInternalServerError, Msg: "服务器内部错误"}
	ErrInvalidParam    = &Error{Code: 10002, HTTPStatus: http.StatusBadRequest, Msg: "请求参数不合法"}
	ErrUnauthorized    = &Error{Code: 10003, HTTPStatus: http.StatusUnauthorized, Msg: "未登录或登录已过期"}
	ErrForbidden       = &Error{Code: 10004, HTTPStatus: http.StatusForbidden, Msg: "无权执行该操作"}
	ErrNotFound        = &Error{Code: 10005, HTTPStatus: http.StatusNotFound, Msg: "资源不存在"}
	ErrTooManyRequests = &Error{Code: 10007, HTTPStatus: http.StatusTooManyRequests, Msg: "请求过于频繁"}
)

// 2xxxx：用户与账号错误码（M1）。
var (
	ErrUsernameExists      = &Error{Code: 20001, HTTPStatus: http.StatusConflict, Msg: "用户名已存在"}
	ErrInvalidCredentials  = &Error{Code: 20002, HTTPStatus: http.StatusUnauthorized, Msg: "用户名或密码错误"}
	ErrUserNotFound        = &Error{Code: 20003, HTTPStatus: http.StatusNotFound, Msg: "用户不存在"}
	ErrInvalidRefreshToken = &Error{Code: 20004, HTTPStatus: http.StatusUnauthorized, Msg: "刷新凭证无效或已过期"}
)

// 5xxxx：游标与 Feed 基础错误码（供内部 cursor 工具复用）。
var (
	ErrInvalidCursor = &Error{Code: 50001, HTTPStatus: http.StatusBadRequest, Msg: "分页游标无效"}
)

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.err != nil {
		return fmt.Sprintf("code=%d msg=%s: %v", e.Code, e.Msg, e.err)
	}
	return fmt.Sprintf("code=%d msg=%s", e.Code, e.Msg)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// Is 按业务码匹配错误，确保 Wrap/WithMessage 后的实例仍可被 errors.Is 识别。
func (e *Error) Is(target error) bool {
	if e == nil || target == nil {
		return false
	}
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// WithMessage 返回副本并覆盖对外安全提示文案，不修改包级哨兵对象。
func (e *Error) WithMessage(msg string) *Error {
	return &Error{
		Code:       e.Code,
		HTTPStatus: e.HTTPStatus,
		Msg:        msg,
		err:        e.err,
	}
}

// Wrap 返回副本并包装底层错误（仅用于服务端日志追踪）。
func (e *Error) Wrap(err error) *Error {
	return &Error{
		Code:       e.Code,
		HTTPStatus: e.HTTPStatus,
		Msg:        e.Msg,
		err:        err,
	}
}

// FromError 将任意 error 转换为 *Error；未知错误一律归并为 ErrInternal，防止泄露内部细节。
func FromError(err error) *Error {
	if err == nil {
		return nil
	}
	var bizErr *Error
	if errors.As(err, &bizErr) {
		return bizErr
	}
	return ErrInternal.Wrap(err)
}
