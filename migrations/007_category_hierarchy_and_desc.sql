SET NAMES utf8mb4;

ALTER TABLE categories
    ADD COLUMN parent_id   BIGINT UNSIGNED NOT NULL DEFAULT 0  COMMENT '父级大分类 ID（0 表示一级大分类）' AFTER id,
    ADD COLUMN description VARCHAR(300)    NOT NULL DEFAULT '' COMMENT '分类/板块简介' AFTER name,
    ADD KEY idx_parent_sort (parent_id, sort_no, id);

-- 为现有预置一级大分类补充简介
UPDATE categories SET description = '汇集二次元风格壁纸，动画、漫画、国漫与角色插画都在这里，按作品板块慢慢逛。' WHERE slug = 'anime' AND description = '';
UPDATE categories SET description = '山川湖海、雪山极光、海岛日落与四季光影，收录高分辨率自然风光摄影与宽屏原图。' WHERE slug = 'landscape' AND description = '';
UPDATE categories SET description = '霓虹都市、未来机甲、深空星际与科幻概念设计，极具氛围感的暗房视觉体验。' WHERE slug = 'cyberpunk' AND description = '';
UPDATE categories SET description = '克制留白、几何构图与低饱和色彩，专为专注桌面与极简主义爱好者甄选。' WHERE slug = 'minimal' AND description = '';
UPDATE categories SET description = '主机、PC 与二次元热门游戏官方原画、CG 场景与同人精选壁纸。' WHERE slug = 'game' AND description = '';
UPDATE categories SET description = '独立画师原创数字插画、概念艺术与绘本风高清作品合集。' WHERE slug = 'illustration' AND description = '';
UPDATE categories SET description = '现代摩天楼、街头夜景、复古街巷与光影几何建筑摄影。' WHERE slug = 'architecture' AND description = '';
UPDATE categories SET description = '治愈系猫咪、柴犬、野生动物与自然微距瞬间。' WHERE slug = 'animal' AND description = '';
UPDATE categories SET description = '流体渲染、3D 光影材质、暗黑纹理与抽象视觉实验作品。' WHERE slug = 'abstract' AND description = '';
UPDATE categories SET description = '跨界创意、汽车机械、人文纪实与其他难以归类的宝藏高清壁纸。' WHERE slug = 'other' AND description = '';

-- 预置一批二级小分类（作品/主题板块，幂等）
INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'genshin', '原神', '提瓦特大陆角色立绘、绝美场景摄影与高帧率同人壁纸。', 10 FROM categories WHERE slug = 'anime';
INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'blue-archive', '蔚蓝档案', '基沃托斯学院日常、角色大厅与清新治愈系同人插画。', 20 FROM categories WHERE slug = 'anime';
INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'chainsaw-man', '电锯人', '高张力暗黑漫画风、剧照美学与角色精选壁纸。', 30 FROM categories WHERE slug = 'anime';
INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'wuthering-waves', '鸣潮', '索拉里斯废土美学、共鸣者角色与高对比度光影原画。', 40 FROM categories WHERE slug = 'anime';

INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'aurora-snow', '雪山极光', '北欧极夜、雪顶冰川与冷冽清透的 4K 风景原图。', 10 FROM categories WHERE slug = 'landscape';
INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'ocean-sunset', '海岛日落', '海岸晚霞、浪花礁石与温柔暮色宽屏壁纸。', 20 FROM categories WHERE slug = 'landscape';
INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'forest-mist', '森林晨雾', '幽深林海、薄雾日光与静谧绿意治愈桌面。', 30 FROM categories WHERE slug = 'landscape';

INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'neon-city', '霓虹都市', '雨夜霓虹、高楼雨幕与赛博街头氛围感壁纸。', 10 FROM categories WHERE slug = 'cyberpunk';
INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'deep-space', '深空星际', '星云黑洞、异星地貌与宇宙尺度震撼视觉。', 20 FROM categories WHERE slug = 'cyberpunk';

INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'darkroom-minimal', '暗房极简', '纯黑 OLED 友好、微弱光影与极低干扰的高级桌面。', 10 FROM categories WHERE slug = 'minimal';
INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'elden-ring', '艾尔登法环', '交界地史诗场景、黄金树与暗黑奇幻艺术原画。', 10 FROM categories WHERE slug = 'game';
INSERT IGNORE INTO categories (parent_id, slug, name, description, sort_no)
SELECT id, 'cyberpunk-2077', '赛博朋克2077', '夜之城街头摄影、载具与高对比光影截图。', 20 FROM categories WHERE slug = 'game';
