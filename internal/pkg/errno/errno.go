// Package errno 定义对外业务错误码。分段：通用 1xxxx、用户 2xxxx、帖子 3xxxx、关系 4xxxx、Feed 5xxxx。
package errno

import "fmt"

type Error struct {
	Code    int
	Message string
	HTTP    int
}

func (e *Error) Error() string { return fmt.Sprintf("errno %d: %s", e.Code, e.Message) }

// WithMsg 返回替换了提示文案的副本。
func (e *Error) WithMsg(msg string) *Error {
	return &Error{Code: e.Code, Message: msg, HTTP: e.HTTP}
}

var (
	OK           = &Error{Code: 0, Message: "ok", HTTP: 200}
	ErrInternal  = &Error{Code: 10001, Message: "服务繁忙，请稍后再试", HTTP: 500}
	ErrParam     = &Error{Code: 10002, Message: "参数错误", HTTP: 400}
	ErrUnauth    = &Error{Code: 10003, Message: "未登录", HTTP: 401}
	ErrNotFound  = &Error{Code: 10004, Message: "资源不存在", HTTP: 404}
	ErrBadCursor = &Error{Code: 10005, Message: "游标无效", HTTP: 400}
	ErrForbidden = &Error{Code: 10006, Message: "无权操作", HTTP: 403}

	ErrUsernameTaken      = &Error{Code: 20001, Message: "用户名已存在", HTTP: 400}
	ErrInvalidCredentials = &Error{Code: 20002, Message: "用户名或密码错误", HTTP: 400}
	ErrUserNotFound       = &Error{Code: 20003, Message: "用户不存在", HTTP: 404}
	ErrSessionExpired     = &Error{Code: 20004, Message: "登录已过期，请重新登录", HTTP: 401}

	ErrPostNotFound     = &Error{Code: 30001, Message: "帖子不存在", HTTP: 404}
	ErrPostContentLong  = &Error{Code: 30002, Message: "文案过长", HTTP: 400}
	ErrPostImageCount   = &Error{Code: 30003, Message: "图片数量需为 1~9 张", HTTP: 400}
	ErrPostImageInvalid = &Error{Code: 30004, Message: "图片无效", HTTP: 400}
)
