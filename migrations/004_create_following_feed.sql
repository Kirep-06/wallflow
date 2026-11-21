-- 04 关注流：通用本地事务消息表、帖子覆盖索引与大 V 标识

-- 1. 通用 Transactional Outbox 表（保障事件与业务事务原子提交，可供 M04 及后续模块复用）
CREATE TABLE IF NOT EXISTS outbox_events (
  id            BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT,
  event_id      VARCHAR(64)       NOT NULL COMMENT '全局唯一事件 ID，如 post.created:12345',
  exchange_name VARCHAR(64)       NOT NULL COMMENT '目标 RabbitMQ Exchange',
  routing_key   VARCHAR(64)       NOT NULL COMMENT '目标 Routing Key，如 post.created',
  payload       JSON              NOT NULL COMMENT '事件 JSON 载荷',
  status        TINYINT           NOT NULL DEFAULT 0 COMMENT '0 待投递 1 已投递 2 投递失败(死信)',
  retry_count   SMALLINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '已失败重试次数',
  next_retry_at DATETIME(3)       NOT NULL COMMENT '下次可捞取时间（兼作多实例捞取租约）',
  last_error    VARCHAR(255)      NOT NULL DEFAULT '',
  created_at    DATETIME(3)       NOT NULL,
  updated_at    DATETIME(3)       NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_event_id (event_id),
  KEY idx_status_retry (status, next_retry_at, id),
  KEY idx_status_updated (status, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 2. 为 posts 补充覆盖索引，支撑关注流冷回源、收件箱/发件箱重建及用户主页帖子列表零回表查询
ALTER TABLE posts
  ADD KEY idx_user_status_id (user_id, status, id);

-- 3. 在 user_follow_stats 持久化大 V 标识（只升不自动降），并支持覆盖索引快速拉取全站大 V 集合
ALTER TABLE user_follow_stats
  ADD COLUMN is_bigv TINYINT NOT NULL DEFAULT 0 COMMENT '0 普通作者(写扩散) 1 大V(读扩散)' AFTER follower_count,
  ADD KEY idx_bigv (is_bigv, user_id);

-- 4. 存量数据平滑回填（若 M03 先于 M04 上线运营，补齐已有大 V 标记）
UPDATE user_follow_stats
SET is_bigv = 1
WHERE follower_count >= 5000 AND is_bigv = 0;
