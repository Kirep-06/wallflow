CREATE TABLE IF NOT EXISTS users (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '用户主键',
    username      VARCHAR(32)     NOT NULL                COMMENT '登录用户名（大小写不敏感唯一）',
    password_hash VARCHAR(255)    NOT NULL                COMMENT 'bcrypt 密码哈希',
    nickname      VARCHAR(32)     NOT NULL                COMMENT '展示昵称',
    avatar_key    VARCHAR(255)    NOT NULL DEFAULT ''     COMMENT '头像对象存储相对 key，空串表示默认头像',
    bio           VARCHAR(200)    NOT NULL DEFAULT ''     COMMENT '个人简介',
    created_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户基础表';
