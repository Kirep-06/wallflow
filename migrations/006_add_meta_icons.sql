SET NAMES utf8mb4;

ALTER TABLE categories
    ADD COLUMN icon_key VARCHAR(255) NOT NULL DEFAULT '' COMMENT '分类图标对象存储相对 key' AFTER sort_no;

ALTER TABLE tags
    ADD COLUMN icon_key VARCHAR(255) NOT NULL DEFAULT '' COMMENT '话题/标签图标对象存储相对 key' AFTER use_count;
