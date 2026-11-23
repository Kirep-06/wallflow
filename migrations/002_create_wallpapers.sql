CREATE TABLE IF NOT EXISTS categories (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '分类主键',
    slug       VARCHAR(32)     NOT NULL                COMMENT '英文唯一标识',
    name       VARCHAR(32)     NOT NULL                COMMENT '中文展示名',
    sort_no    INT             NOT NULL DEFAULT 0      COMMENT '展示顺序（升序）',
    created_at DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_slug (slug),
    KEY idx_sort (sort_no, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='壁纸分类表';

CREATE TABLE IF NOT EXISTS tags (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '标签主键',
    name       VARCHAR(24)     NOT NULL                COMMENT '归一化标签名（小写唯一）',
    use_count  BIGINT UNSIGNED NOT NULL DEFAULT 0      COMMENT '当前正常壁纸关联计数',
    created_at DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_name (name),
    KEY idx_hot_tags (use_count DESC, id ASC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='壁纸标签表';

CREATE TABLE IF NOT EXISTS wallpapers (
    id             BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT COMMENT '壁纸主键',
    user_id        BIGINT UNSIGNED  NOT NULL                COMMENT '投稿创作者 ID',
    category_id    BIGINT UNSIGNED  NOT NULL                COMMENT '所属分类 ID',
    title          VARCHAR(80)      NOT NULL                COMMENT '壁纸标题',
    description    VARCHAR(500)     NOT NULL DEFAULT ''     COMMENT '壁纸简介',
    object_key     VARCHAR(255)     NOT NULL                COMMENT '对象存储相对 key（不含域名，扩展名严格绑定真实格式）',
    width          INT UNSIGNED     NOT NULL                COMMENT '原图宽度(px)',
    height         INT UNSIGNED     NOT NULL                COMMENT '原图高度(px)',
    orientation    TINYINT UNSIGNED NOT NULL                COMMENT '1-横屏 2-竖屏 3-方图',
    is_4k          TINYINT UNSIGNED NOT NULL DEFAULT 0      COMMENT '0-否 1-是',
    size_bytes     BIGINT UNSIGNED  NOT NULL                COMMENT '文件字节大小',
    status         TINYINT UNSIGNED NOT NULL DEFAULT 1      COMMENT '1-正常 2-已删除',
    created_at     DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_object_key (object_key),
    KEY idx_status_id (status, id),
    KEY idx_user_status_id (user_id, status, id),
    KEY idx_status_category_id (status, category_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='壁纸静态主表';

CREATE TABLE IF NOT EXISTS wallpaper_tags (
    wallpaper_id BIGINT UNSIGNED NOT NULL COMMENT '壁纸 ID',
    tag_id       BIGINT UNSIGNED NOT NULL COMMENT '标签 ID',
    PRIMARY KEY (wallpaper_id, tag_id),
    KEY idx_tag_wp (tag_id, wallpaper_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='壁纸标签关联表';

-- 预置 10 个高频壁纸分类（幂等）
INSERT IGNORE INTO categories (slug, name, sort_no) VALUES
    ('anime',        '动漫二次元', 10),
    ('landscape',    '自然风景',   20),
    ('cyberpunk',    '赛博科幻',   30),
    ('minimal',      '极简纯粹',   40),
    ('game',         '游戏原画',   50),
    ('illustration', '插画艺术',   60),
    ('architecture', '城市建筑',   70),
    ('animal',       '萌宠动物',   80),
    ('abstract',     '抽象光影',   90),
    ('other',        '其他精选',   100);
