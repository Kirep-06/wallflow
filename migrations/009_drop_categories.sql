-- 009_drop_categories.sql
-- 彻底移除内容分类板块（categories 表及 wallpapers.category_id）与画质分档列（is_4k），
-- 转向 Pixiv 风格纯标签驱动静态图片社区。

ALTER TABLE wallpapers
    DROP INDEX idx_status_category_id,
    DROP COLUMN category_id,
    DROP COLUMN is_4k;

ALTER TABLE wallpaper_images
    DROP COLUMN is_4k;

DROP TABLE IF EXISTS categories;
