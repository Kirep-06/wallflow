package service

import (
	"strings"
	"unicode/utf8"

	"wallpaper-feed/internal/pkg/errno"
)

const maxNicknameRunes = 32

func validateNickname(raw string) (string, error) {
	nick := strings.TrimSpace(raw)
	n := utf8.RuneCountInString(nick)
	if n < 1 || n > maxNicknameRunes {
		return "", errno.ErrInvalidParam.WithMessage("昵称长度需为 1~32 个字符")
	}
	return nick, nil
}
