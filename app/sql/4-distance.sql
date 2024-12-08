-- 1. `updated_at`列を追加
-- 2. `total_distance`列を追加
ALTER TABLE
  chair_locations
ADD
  COLUMN updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
ADD
  COLUMN total_distance INTEGER NOT NULL DEFAULT 0 COMMENT '移動距離';