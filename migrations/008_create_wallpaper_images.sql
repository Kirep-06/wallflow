SET NAMES utf8mb4;

CREATE TABLE IF NOT EXISTS wallpaper_images (
    id           BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT COMMENT '壁纸图片主键',
    wallpaper_id BIGINT UNSIGNED  NOT NULL                COMMENT '所属帖子/壁纸 ID',
    sort_no      INT UNSIGNED     NOT NULL DEFAULT 0      COMMENT '展示顺序（从 0 递增）',
    object_key   VARCHAR(255)     NOT NULL                COMMENT '对象存储相对 key',
    width        INT UNSIGNED     NOT NULL                COMMENT '原图宽度(px)',
    height       INT UNSIGNED     NOT NULL                COMMENT '原图高度(px)',
    orientation  TINYINT UNSIGNED NOT NULL                COMMENT '1-横屏 2-竖屏 3-方图',
    is_4k        TINYINT UNSIGNED NOT NULL DEFAULT 0      COMMENT '0-否 1-是',
    size_bytes   BIGINT UNSIGNED  NOT NULL                COMMENT '文件字节大小',
    created_at   DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_object_key (object_key),
    KEY idx_wp_sort (wallpaper_id, sort_no, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='壁纸帖子多图子表';

-- 幂等回填历史单图壁纸为 sort_no = 0 的首图记录
INSERT IGNORE INTO wallpaper_images (
    wallpaper_id, sort_no, object_key, width, height, orientation, is_4k, size_bytes, created_at
)
SELECT
    id, 0, object_key, width, height, orientation, is_4k, size_bytes, created_at
FROM wallpapers;
