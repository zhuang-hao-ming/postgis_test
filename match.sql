DO
$$

<<first_block>>
DECLARE
	a_gps_point gps_log_valid.geom%TYPE;
	
BEGIN
	SELECT geom INTO a_gps_point FROM gps_log_valid WHERE id=1088530;
	SELECT ST_ClosestPoint(geom, a_gps_point) FROM road WHERE ST_DWithin(geom, a_gps_point, 30) LIMIT 5;
	
END first_block;

$$

SELECT * FROM road LIMIT 1;

#
# select closest point
#
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