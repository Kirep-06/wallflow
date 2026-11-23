-- M5: 事务发件箱事件表（Transactional Outbox）
-- 依赖: MySQL 8.0+ (InnoDB / utf8mb4 / FOR UPDATE SKIP LOCKED)

CREATE TABLE IF NOT EXISTS outbox_events (
    id            BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT COMMENT '自增主键（保证同时间戳稳定排序）',
    event_id      VARCHAR(64)      NOT NULL                COMMENT '全局唯一事件 ID（用于幂等与 MQ MessageId）',
    exchange_name VARCHAR(64)      NOT NULL                COMMENT '目标 RabbitMQ 交换机',
    routing_key   VARCHAR(64)      NOT NULL                COMMENT '目标路由键（如 wallpaper.published）',
    payload       JSON             NOT NULL                COMMENT '事件 JSON 载荷',
    status        TINYINT UNSIGNED NOT NULL DEFAULT 0      COMMENT '0-待投递 1-已投递 2-死信（超重试上限）',
    retry_count   INT UNSIGNED     NOT NULL DEFAULT 0      COMMENT '已失败重试次数',
    next_retry_at DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '下次允许捞取投递时间（UTC）',
    last_error    VARCHAR(500)     NOT NULL DEFAULT ''     COMMENT '最近一次投递失败摘要（截断保存）',
    created_at    DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at    DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_event_id (event_id),
    KEY idx_status_retry (status, next_retry_at, id),
    KEY idx_status_updated (status, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='事务发件箱事件表';
