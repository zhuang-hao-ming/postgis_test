

-- idicate whether two consective gps_logs are belong to the same track
CREATE OR REPLACE FUNCTION test_time_dis_constrain(pre_x FLOAT, pre_y FLOAT, now_x FLOAT, now_y FLOAT, pre_time TIMESTAMP, now_time TIMESTAMP, time_threshold INTEGER DEFAULT 120, speed_threshold INTEGER DEFAULT 33)
RETURNS BOOLEAN 
AS
$$
DECLARE
	delta INTEGER := 0;
	dis FLOAT := 0;
BEGIN
	-- the interval of two logs
	SELECT EXTRACT (EPOCH FROM (pre_time - now_time)) INTO delta;
	delta := abs(delta);

	-- debug message
	-- RAISE INFO 'pre_time %, now_time %, pre_x % , pre_y %, now_x %, now_y % ,delta %', pre_time, now_time, pre_x, pre_y, now_x, now_y, delta;

	
	IF delta > time_threshold THEN
		return false;
	END IF;
	
	dis = (pre_x - now_x) ^ 2 + (pre_y - now_y) ^ 2;
	
	IF dis > (delta * speed_threshold) ^ 2 THEN
		return false;
	END IF;
	return true;	
END;

$$
LANGUAGE plpgsql;




--- make track table
--- this function iterate the gps_log_valid table in order and make track according to criterias given by test_time_dis_constrain function
CREATE OR REPLACE FUNCTION make_track()
RETURNS VOID 
AS $$
DECLARE 
	log_ids INT[];
	pre_log RECORD;
	cur_log RECORD;
	cur_logs CURSOR FOR SELECT id,log_time, car_id, direction AS v, ST_X(geom) AS x, ST_Y(geom) AS y  FROM gps_log_valid ORDER BY car_id, log_time;
BEGIN
	-- Open the gps log cursor
	OPEN cur_logs;
	-- fetch the first log
	FETCH cur_logs INTO pre_log;
	log_ids := ARRAY[pre_log.id];
	LOOP
		-- fetch row into the rec_log
		FETCH cur_logs INTO cur_log;
		EXIT WHEN NOT FOUND;
					
		IF test_time_dis_constrain(pre_log.x, pre_log.y, cur_log.x, cur_log.y, pre_log.log_time, cur_log.log_time) THEN
			log_ids := array_append(log_ids, cur_log.id);
			pre_log := cur_log;
			--RAISE INFO 'append %', cur_log.id;
			
		ELSE	
			--RAISE INFO 'insert %', log_ids;
			INSERT INTO tracks_1(points) VALUES(log_ids);
			log_ids := ARRAY[cur_log.id];
			pre_log := cur_log;
		END IF;
				
	END LOOP;

	IF array_length(log_ids, 1) > 0 THEN	
		INSERT INTO tracks_1(points) VALUES(log_ids);
		--RAISE INFO 'end insert %', log_ids;
	END IF;
  
   -- Close the cursor
	CLOSE cur_logs;

	RETURN;
END; $$
LANGUAGE plpgsql;


-- create the track table
CREATE TABLE tracks_1
(
id SERIAL PRIMARY KEY,
points INTEGER[]
)
-- invode the function
SELECT make_track();















