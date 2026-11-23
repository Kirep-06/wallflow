-- M6: 互动体系与高频计数异步聚合（冷热分离，不改动 wallpapers 静态主表）

CREATE TABLE IF NOT EXISTS wallpaper_stats (
    wallpaper_id   BIGINT UNSIGNED NOT NULL             COMMENT '壁纸 ID（与 wallpapers.id 1:1 逻辑关联）',
    like_count     BIGINT UNSIGNED NOT NULL DEFAULT 0   COMMENT '点赞总数',
    fav_count      BIGINT UNSIGNED NOT NULL DEFAULT 0   COMMENT '收藏总数',
    download_count BIGINT UNSIGNED NOT NULL DEFAULT 0   COMMENT '原图下载总数',
    comment_count  BIGINT UNSIGNED NOT NULL DEFAULT 0   COMMENT '正常状态评论总数',
    updated_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (wallpaper_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='壁纸互动计数冷热分离表';

CREATE TABLE IF NOT EXISTS wallpaper_likes (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '关系主键（我赞过的倒序游标）',
    user_id      BIGINT UNSIGNED NOT NULL                COMMENT '点赞用户 ID',
    wallpaper_id BIGINT UNSIGNED NOT NULL                COMMENT '壁纸 ID',
    created_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_user_wp (user_id, wallpaper_id),
    KEY idx_user_list (user_id, id, wallpaper_id),
    KEY idx_wp (wallpaper_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='壁纸点赞关系表';

CREATE TABLE IF NOT EXISTS wallpaper_favorites (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '关系主键（我的收藏倒序游标）',
    user_id      BIGINT UNSIGNED NOT NULL                COMMENT '收藏用户 ID',
    wallpaper_id BIGINT UNSIGNED NOT NULL                COMMENT '壁纸 ID',
    created_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_user_wp (user_id, wallpaper_id),
    KEY idx_user_list (user_id, id, wallpaper_id),
    KEY idx_wp (wallpaper_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='壁纸收藏关系表';

CREATE TABLE IF NOT EXISTS comments (
    id           BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT COMMENT '评论主键（倒序游标）',
    wallpaper_id BIGINT UNSIGNED  NOT NULL                COMMENT '所属壁纸 ID',
    user_id      BIGINT UNSIGNED  NOT NULL                COMMENT '评论作者用户 ID',
    content      VARCHAR(500)     NOT NULL                COMMENT '一级评论纯文本内容（1~500 字符）',
    status       TINYINT UNSIGNED NOT NULL DEFAULT 1      COMMENT '1-正常 2-已删除',
    created_at   DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    KEY idx_wp_status_id (wallpaper_id, status, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='壁纸一级评论表';
