ALTER TABLE chairs 
ADD COLUMN total_distance INT NOT NULL DEFAULT 0 COMMENT '累積走行距離',
ADD COLUMN total_distance_updated_at DATETIME(6) NULL COMMENT '累積距離更新日時';
