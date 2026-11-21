-- 01 发帖 → 最新流
CREATE TABLE IF NOT EXISTS posts (
  id           BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  user_id      BIGINT UNSIGNED  NOT NULL,
  content      VARCHAR(2000)    NOT NULL DEFAULT '',
  image_count  TINYINT UNSIGNED NOT NULL,
  status       TINYINT          NOT NULL DEFAULT 1 COMMENT '1 正常 2 删除',
  created_at   DATETIME(3)      NOT NULL,
  updated_at   DATETIME(3)      NOT NULL,
  PRIMARY KEY (id),
  KEY idx_status_id (status, id),
  KEY idx_user_id (user_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS post_images (
  id          BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  post_id     BIGINT UNSIGNED  NOT NULL,
  sort_no     TINYINT UNSIGNED NOT NULL,
  object_key  VARCHAR(255)     NOT NULL,
  width       INT UNSIGNED     NOT NULL,
  height      INT UNSIGNED     NOT NULL,
  size_bytes  INT UNSIGNED     NOT NULL,
  mime        VARCHAR(32)      NOT NULL,
  created_at  DATETIME(3)      NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_post_sort (post_id, sort_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
