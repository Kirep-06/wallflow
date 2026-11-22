package repository

import (
	"errors"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

const mysqlErrDuplicateEntry uint16 = 1062

func isDuplicateKeyError(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
		return true
	}
	return false
}
