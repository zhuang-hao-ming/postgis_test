# 创建gps_log表
# 
CREATE TABLE gps_log
(
    id SERIAL PRIMARY KEY,
    log_time TIMESTAMP NOT NULL,
    unknown_1 TEXT,
    car_id TEXT NOT NULL,
    velocity SMALLINT NOT NULL,
    direction SMALLINT NOT NULL,
    on_service BOOLEAN NOT NULL,
    is_valid BOOLEAN NOT NULL    
);
# 往gps_log表插入几何字段
SELECT AddGeometryColumn('public', 'gps_log', 'geom', 32649, 'POINT', 2);

# 插入数据后，建立索引， 并且cluster
CREATE INDEX gps_log_gidx ON gps_log USING GIST(geom);
VACUUM ANALYZE gps_log;
CLUSTER gps_log USING gps_log_gidx;
ANALYZE gps_log;

# 对原始的gps_log进行清理
# 只要7：30-8：30
# 载客的
# 在路网的30米缓冲区内
CREATE TABLE 
	gps_log_valid AS
SELECT DISTINCT ON (gp.id) gp.* 
FROM 
	gps_log gp, road_buffer r 
WHERE 
	gp.log_time::time between '07:30:00' AND '08:30:00' AND 
	gp.velocity >= 0 AND
	gp.direction >= 0 AND 
	gp.is_valid is true AND
	gp.on_service is true AND
	ST_Contains(r.geom, gp.geom);
# 插入数据后, 建立索引， 并且cluster
CREATE INDEX gps_log_valid_gidx ON gps_log_valid USING GIST(geom);
VACUUM ANALYZE gps_log_valid;
CLUSTER gps_log_valid USING gps_log_valid_gidx;
ANALYZE gps_log_valid;



# 轨迹表
CREATE TABLE tracks 
(
	id SERIAL PRIMARY KEY,
	points INTEGER []
);
CREATE INDEX tracks_idx ON tracks(id);
VACUUM ANALYZE tracks;
CLUSTER tracks USING tracks_idx;
ANALYZE tracks;






# 验证轨迹
SELECT gp.geom, gp.direction AS v FROM gps_log_valid gp, tracks t WHERE t.id=15 AND gp.id = ANY(t.points) ORDER BY log_time;










