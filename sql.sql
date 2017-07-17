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
	gps.direction AS v,
	r.source AS source,
	r.target AS target
	
FROM 
	shenzhen_network r, 
	gps_log_valid gps 
WHERE 
	gps.id in (1494167,1502698,1511616,1520537,1529545,1538291,1547068,1565190,1574253,1583447,1592171,1601214) AND
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







-- 得到线上两个点之间的距离

WITH
data AS (
	SELECT ST_GeomFromText('POINT(796449.433546537 2502766.24045783)', 32649) pta,
		ST_GeomFromText('POINT(795563.273692348 2503973.71146982)', 32649) ptb,
		ST_GeometryN(geom, 1)::geometry(linestring, 32649) line FROM shenzhen_network WHERE gid = 22205
)
SELECT 
	ST_Length(
		ST_LineSubstring(
			line,
			least(ST_LineLocatePoint(line, pta), ST_LineLocatePoint(line, ptb)),
			greatest(ST_LineLocatePoint(line, pta), ST_LineLocatePoint(line, ptb))
			)
		)
FROM 
	data;







-- distance from a point on the line to the line's start vertext

WITH
data AS (
SELECT 
	ST_GeomFromText('POINT(796449.433546537 2502766.24045783)', 32649) pta,
	ST_GeometryN(geom, 1)::geometry(linestring, 32649) line FROM shenzhen_network WHERE gid=22205
)
SELECT  ST_LineLocatePoint(line, pta), ST_Length(line) FROM data;


-- distance between two vertex

SELECT *
FROM pgr_dijkstraCost('
    SELECT gid AS id,
         source,
         target,
         ST_Length(geom) AS cost
        FROM shenzhen_network',
    %s,
    %s,
    directed := false);


SELECT ST_LineLocatePoint(line, pta)







------- create poi table



CREATE TABLE points_of_interest(
    pid BIGSERIAL,
    x FLOAT,
    y FLOAT,
    edge_id BIGINT,
    side CHAR,
    fraction FLOAT,
    geom geometry    
);


-- insert one point
WITH
data AS (
	SELECT 
		799029.028296921 x,
		2500355.51642549 y,
		22203 edge_id,
		'b' side,
		ST_GeomFromText('POINT(799029.028296921 2500355.51642549)', 32649) pta,		
		ST_GeometryN(geom, 1)::geometry(linestring, 32649) line FROM shenzhen_network WHERE gid = 22203
)
INSERT INTO points_of_interest(
	x,
	y,
	edge_id,
	side,
	fraction,
	geom) 
SELECT x, y, edge_id, side, ST_LineLocatePoint(line, pta), pta FROM data;

--- insert one point
WITH
data AS (
	SELECT 
		797766.309896996 x,
		2501670.61053837 y,
		22204 edge_id,
		'b' side,
		ST_GeomFromText('POINT(797766.309896996 2501670.61053837)', 32649) pta,		
		ST_GeometryN(geom, 1)::geometry(linestring, 32649) line FROM shenzhen_network WHERE gid = 22204
)
INSERT INTO points_of_interest(
	x,
	y,
	edge_id,
	side,
	fraction,
	geom)
SELECT x, y, edge_id, side, ST_LineLocatePoint(line, pta), pta FROM data;

--- path between two point on the middle of a line
SELECT * FROM pgr_withPointsCost (
        'SELECT 
		gid  AS id,
		source,
		target,
		ST_Length(geom) as cost,
		ST_Length(geom) as reverse_cost
	FROM
		shenzhen_network ORDER BY gid',
        'SELECT pid, edge_id, fraction, side from points_of_interest',
        -4, -5);





SELECT * FROM points_of_interest;
DELETE FROM points_of_interest;





-- extract linestring form multilinestring
SELECT AddGeometryColumn ('public','shenzhen_network','geom_l',32649,'LINESTRING',2);
UPDATE shenzhen_network SET geom_l = ST_GeometryN(geom, 1)::geometry(linestring, 32649)





CREATE TABLE points_of_interest(
    pid BIGSERIAL,    
    log_x FLOAT,
    log_y FLOAT,
    p_x FLOAT,
    p_y FLOAT,
    v FLOAT,
    gps_log_id BIGINT, 
    edge_id BIGINT,
    side CHAR,
    fraction FLOAT,
    geom geometry    
);


SELECT 
	ST_X(gps.geom) AS log_x,
	ST_Y(gps.geom) AS log_y,
	ST_X(ST_ClosestPoint(r.geom, gps.geom)) AS p_x,
	ST_Y(ST_ClosestPoint(r.geom, gps.geom)) AS p_y,
	gps.direction AS v,
	gps.id AS gps_log_id,
	r.gid AS edge_id,
	'b' AS side,
	ST_LineLocatePoint(r.geom_l, gps.geom) AS fraction,
	ST_ClosestPoint(r.geom, gps.geom) AS geom,
	r.source AS source,
	r.target AS target,
	t.id AS track_id
	
FROM 
	shenzhen_network r, 
	gps_log_valid gps,
	tracks t
WHERE 
	array_length(t.points, 1) > 10 AND
	gps.id = ANY(t.points)  AND
	ST_DWithin(gps.geom, r.geom,  30)
LIMIT 10000;




SELECT * FROM tracks LIMIT 1;



