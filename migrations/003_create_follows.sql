-- 03 关注关系与关注计数
CREATE TABLE IF NOT EXISTS user_follows (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  follower_id  BIGINT UNSIGNED NOT NULL COMMENT '关注者用户 ID',
  followee_id  BIGINT UNSIGNED NOT NULL COMMENT '被关注者用户 ID',
  created_at   DATETIME(3)     NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_follower_followee (follower_id, followee_id),
  KEY idx_follower_list (follower_id, id, followee_id),
  KEY idx_followee_list (followee_id, id, follower_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS user_follow_stats (
  user_id          BIGINT UNSIGNED NOT NULL,
  following_count  INT UNSIGNED    NOT NULL DEFAULT 0,
  follower_count   INT UNSIGNED    NOT NULL DEFAULT 0,
  updated_at       DATETIME(3)     NOT NULL,
  PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
