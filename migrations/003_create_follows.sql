CREATE TABLE IF NOT EXISTS user_follows (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '关注记录主键（兼作列表倒序游标）',
    follower_id BIGINT UNSIGNED NOT NULL                COMMENT '关注者用户 ID',
    followee_id BIGINT UNSIGNED NOT NULL                COMMENT '被关注者用户 ID',
    created_at  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_follower_followee (follower_id, followee_id),
    KEY idx_follower_list (follower_id, id, followee_id),
    KEY idx_followee_list (followee_id, id, follower_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户关注关系表';

CREATE TABLE IF NOT EXISTS user_follow_stats (
    user_id         BIGINT UNSIGNED  NOT NULL           COMMENT '用户 ID（与 users.id 1:1 懒初始化）',
    following_count BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '关注数',
    follower_count  BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '粉丝数',
    is_bigv         TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '0-普通作者 1-大V（单向晋升）',
    updated_at      DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id),
    KEY idx_bigv (is_bigv, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户关注统计与大V标记表';
