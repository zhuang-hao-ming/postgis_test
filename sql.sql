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






-- 验证轨迹
SELECT gp.geom, gp.direction AS v FROM gps_log_valid gp, tracks t WHERE t.id=6584 AND gp.id = ANY(t.points) ORDER BY log_time;
-- 导出验证轨迹
-- pgsql2shp -f track.shp -h localhost -u postgres -P 123456 road_gps "SELECT gp.geom, gp.direction AS v FROM gps_log_valid gp, tracks t WHERE t.id=6584 AND gp.id = ANY(t.points) ORDER BY log_time;"


SELECT * FROM tracks WHERE array_length(points, 1) > 10 LIMIT 1;



-- select closest point

SELECT 
	ST_X(gps.geom) AS log_x,
	ST_Y(gps.geom) AS log_y,
	ST_X(ST_ClosestPoint(r.geom, gps.geom)) AS p_x,
	ST_Y(ST_ClosestPoint(r.geom, gps.geom)) AS p_y,
	r.gid AS line_id,
	gps.id AS log_id,
	gps.direction AS v
	
FROM 
	road r, 
	gps_log_valid gps 
WHERE 
	gps.id in (1088530, 1105285, 1106874, 1118795, 1132178, 1145744, 1147531) AND
	ST_DWithin(gps.geom, r.geom,  30);

-- 导入深圳路网数据
-- shp2pgsql -s 32649 -W GBK shenzhen_network.shp shenzhen_network > shenzhen_network.sql
-- psql -f shenzhen_network.sql road_gps postgres
VACUUM ANALYZE shenzhen_network;


-- 使用pgrouting
CREATE EXTENSION pgrouting;
-- 建立拓扑信息
ALTER TABLE shenzhen_network ADD COLUMN "source" integer;
ALTER TABLE shenzhen_network ADD COLUMN "target" integer;
SELECT pgr_createTopology('shenzhen_network', 1, 'geom', 'gid');


-- 最短路径查询例子
SELECT * FROM pgr_dijkstra('
    SELECT gid AS id,
         source,
         target,
         shape_leng::float AS cost
        FROM shenzhen_network',
    12036, 10544, directed := false);
-- 验证
SELECT routing.seq, network.geom FROM shenzhen_network network, (SELECT * FROM pgr_dijkstra('SELECT gid AS id,source,target,shape_leng::float AS cost FROM shenzhen_network', 12036, 10544, directed := false)) routing WHERE network.gid = routing.edge ORDER BY seq;
--- 导出验证结果
pgsql2shp -f routing.shp -h localhost -u postgres -P 123456 road_gps "SELECT routing.seq, network.geom FROM shenzhen_network network, (SELECT * FROM pgr_dijkstra('SELECT gid AS id,source,target,shape_leng::float AS cost FROM shenzhen_network', 10519, 549, directed := false)) routing WHERE network.gid = routing.edge ORDER BY seq;"


SELECT * FROM tracks WHERE array_length(points, 1) > 10 AND id = 6584 LIMIT 1;


