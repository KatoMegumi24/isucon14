ALTER TABLE chairs 
ADD COLUMN total_distance INT NOT NULL DEFAULT 0 COMMENT '累積走行距離',
ADD COLUMN total_distance_updated_at DATETIME(6) NULL COMMENT '累積距離更新日時',
ADD COLUMN last_longitude INT NULL COMMENT '最後の経度',
ADD COLUMN last_latitude INT NULL COMMENT '最後の緯度';
