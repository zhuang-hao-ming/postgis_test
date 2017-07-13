DROP TABLE gps_log1;
CREATE TABLE gps_log2
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
SELECT AddGeometryColumn('public', 'gps_log2', 'geom', 32649, 'POINT', 2);
SELECT ST_AsText(geom) FROM gps_log1 LIMIT 1;
SELECT COUNT(*) FROM gps_log1;

INSERT INTO gps_log1(log_time, unknown_1, car_id, velocity, direction, on_service, is_valid, geom)
VALUES ('2009-05-01 00:57:54', 'H', '13013814358', 42, 8, False, True, ST_Transform(ST_GeomFromText('POINT(114.076150 22.543683)', 4326), 32649));

SELECT *, ST_AsText(geom) FROM gps_log2 LIMIT 1;

SELECT *, ST_AsText(geom) FROM gps_log ORDER BY car_id, log_time  LIMIT 100;
SELECT COUNT(*) FROM gps_log2;

CREATE INDEX gps_log_gix2 ON gps_log2 USING GIST(geom);


CLUSTER gps_log2 USING gps_log_gix2;
VACUUM ANALYZE gps_log2;
DELETE  FROM gps_log1;

# -----------
# load road shp to pgsql
# cd /path/to/the/shp
# shp2pgsql -s 32649 road_0_5.shp road > road.sql
# psql -f road.sql postgres
# ---------
# add spatial index to road
CREATE INDEX road_gix ON road USING GIST(geom);
# cluster road
CLUSTER road USING road_gix;
# vacuum and analyze road
VACUUM ANALYZE road;

CREATE INDEX road_buffer_gix ON road_buffer USING GIST(geom);
CLUSTER road_buffer USING road_buffer_gix;
VACUUM ANALYZE road_buffer;

SELECT * FROM road_buffer LIMIT 1;
DROP TABLE gps_log_valid;

# 
# create a valid gps_log_table
# where 
#
CREATE TABLE gps_log_valid AS SELECT DISTINCT ON (gp.id) gp.* FROM gps_log2 gp, road_buffer r WHERE gp.velocity >= 0 AND gp.is_valid is true AND gp.on_service is true AND ST_Contains(r.geom, gp.geom);

SELECT log_time, car_id, direction AS v, ST_X(geom) AS x, ST_Y(geom) AS y  FROM gps_log_valid WHERE log_time::time between '07:30:00' AND '08:30:00' ORDER BY car_id, log_time LIMIT 100000 OFFSET 2300000;

SELECT COUNT(*) FROM gps_log_valid WHERE log_time::time between '07:30:00' AND '08:30:00';

CREATE INDEX gps_log_valid_gidx ON gps_log_valid USING GIST(geom);
DROP INDEX gps_log_valid_idx;
CREATE INDEX gps_log_valid_idx ON gps_log_valid (car_id, log_time);
CLUSTER gps_log_valid USING gps_log_valid_idx;
VACUUM ANALYZE gps_log_valid;


CREATE TABLE a_road AS SELECT ST_Union(geom) FROM road; 

SELECT * FROM gps_log2 WHERE ST_DWithin(geom, (SELECT st_union FROM a_road), 30) LIMIT 10000;

SELECT ST_Union(geom) FROM road;
CREATE INDEX a_road_gix ON a_road USING GIST(st_union);
SELECT * FROM a_road;
SELECT * FROM road LIMIT 1;







# tracks table

CREATE TABLE tracks 
(
	id SERIAL PRIMARY KEY,
	points INTEGER []
);

INSERT INTO tracks(points) VALUES
(ARRAY [1,2]),
(ARRAY [3,4,5]);
DELETE FROM tracks;
SELECT COUNT(*) FROM tracks;


SELECT * FROM tracks LIMIT 10;


SELECT * FROM tracks WHERE array_length(points, 1) > 10 LIMIT 1;


SELECT * FROM gps_log_valid WHERE id = ANY(ARRAY [1088530,1105285,1106874,1118795,1132178,1145744,1147531])


SELECT * FROM road WHERE ST_DWithin(geom, (SELECT geom FROM gps_log_valid WHERE id = 1088530), 30) LIMIT 5;

