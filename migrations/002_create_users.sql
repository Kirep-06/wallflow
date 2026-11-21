-- 02 注册 / 登录与用户资料
-- 预检存量帖子作者 ID：Go database/sql 的 LastInsertId() 为有符号 int64，最大可安全回填的主键为 9223372036854775807（2^63-1）。
-- 若 MAX(posts.user_id) >= 9223372036854775807，则下一自增 ID 无法可靠回填，主动报错中止切换。
SET @max_post_uid = (SELECT CAST(COALESCE(MAX(user_id), 0) AS DECIMAL(20, 0)) FROM posts);

SET @precheck_sql = IF(
  @max_post_uid >= 9223372036854775807,
  'SELECT * FROM __migration_002_error_posts_user_id_exceeds_safe_int64_bound__',
  'DO 0'
);

PREPARE stmt_002_precheck FROM @precheck_sql;
EXECUTE stmt_002_precheck;
DEALLOCATE PREPARE stmt_002_precheck;

CREATE TABLE IF NOT EXISTS users (
  id             BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  username       VARCHAR(32)      CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  password_hash  VARCHAR(255)     NOT NULL,
  nickname       VARCHAR(32)      NOT NULL,
  avatar_key     VARCHAR(255)     NOT NULL DEFAULT '',
  bio            VARCHAR(200)     NOT NULL DEFAULT '',
  status         TINYINT          NOT NULL DEFAULT 1 COMMENT '1 正常 2 封禁',
  created_at     DATETIME(3)      NOT NULL,
  updated_at     DATETIME(3)      NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 保护模块 01 存量帖子作者 ID：将 users 下一自增值设为大于 MAX(posts.user_id) 与 MAX(users.id) 的最大值。
-- 空库时从 1 开始；若已达 9223372036854775807（2^63-1），无法继续安全分配下一自增 ID，主动报错中止迁移。
SET @max_user_id  = (SELECT CAST(COALESCE(MAX(id), 0) AS DECIMAL(20, 0)) FROM users);
SET @max_existing = GREATEST(@max_post_uid, @max_user_id);

SET @next_uid_sql = IF(
  @max_existing >= 9223372036854775807,
  'SELECT * FROM __migration_002_error_users_auto_increment_exceeds_safe_int64_bound__',
  CONCAT('ALTER TABLE users AUTO_INCREMENT = ', CAST(@max_existing + 1 AS CHAR))
);

PREPARE stmt_002_auto_inc FROM @next_uid_sql;
EXECUTE stmt_002_auto_inc;
DEALLOCATE PREPARE stmt_002_auto_inc;
