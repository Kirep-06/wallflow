package infra

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"wallpaper-feed/internal/config"
)

const (
	mysqlDefaultMaxOpen  = 50
	mysqlDefaultMaxIdle  = 10
	mysqlConnMaxLifetime = 30 * time.Minute
)

// OpenMySQL 初始化 GORM MySQL 连接池并执行启动连通性检查。
func OpenMySQL(ctx context.Context, cfg config.MySQLConfig, isDev bool) (*gorm.DB, error) {
	logLevel := gormlogger.Warn
	if !isDev {
		logLevel = gormlogger.Error
	}

	dbLogger := gormlogger.New(
		stdlog.New(os.Stdout, "\r\n", stdlog.LstdFlags),
		gormlogger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logLevel,
			IgnoreRecordNotFoundError: true,
			// 强制日志参数化，防止慢查询或错误日志泄露 password_hash 等敏感字段。
			ParameterizedQueries: true,
			Colorful:             false,
		},
	)

	db, err := gorm.Open(mysql.Open(cfg.DSN()), &gorm.Config{
		TranslateError: true,
		Logger:         dbLogger,
	})
	if err != nil {
		return nil, fmt.Errorf("connect mysql: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get mysql sql.DB: %w", err)
	}

	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = mysqlDefaultMaxOpen
	}
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(min(mysqlDefaultMaxIdle, maxOpen))
	sqlDB.SetConnMaxLifetime(mysqlConnMaxLifetime)

	pingCtx, cancel := context.WithTimeout(ctx, startupPingTimeout)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return db, nil
}

// CloseMySQL 优雅关闭 GORM 底层 MySQL 连接池。
func CloseMySQL(db *gorm.DB) error {
	if db == nil {
		return nil
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
