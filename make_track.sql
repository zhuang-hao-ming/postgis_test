


CREATE OR REPLACE FUNCTION test_time_dis_constrain(pre_x FLOAT, pre_y FLOAT, now_x FLOAT, now_y FLOAT, pre_time TIMESTAMP, now_time TIMESTAMP, time_threshold INTEGER DEFAULT 120, speed_threshold INTEGER DEFAULT 33)
RETURNS BOOLEAN 
AS
$$
DECLARE
	delta INTEGER := 0;
	dis FLOAT := 0;
BEGIN
	SELECT EXTRACT (EPOCH FROM (pre_time - now_time)) INTO delta;
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




--- make track

CREATE OR REPLACE FUNCTION make_track()
RETURNS TABLE 
(
   id BIGINT,
   gps_log_ids INT[]
) 
AS $$
DECLARE 
	titles TEXT DEFAULT '';
	uuid BIGINT DEFAULT 1;
	log_ids INT[];
	pre_log RECORD;
	cur_log RECORD;
	cur_logs CURSOR FOR SELECT id,log_time, car_id, direction AS v, ST_X(geom) AS x, ST_Y(geom) AS y  FROM gps_log_valid ORDER BY car_id, log_time LIMIT 10;
BEGIN
	-- Open the gps log cursor
	OPEN cur_logs;
	-- fetch the first log
	FETCH cur_logs INTO pre_log;
	log_ids := ARRAY[pre_log.id];
	LOOP
		uuid := uuid + 1;
		-- fetch row into the rec_log
		FETCH cur_logs INTO cur_log;
		
		IF NOT cur_log THEN
			IF array_length(log_ids, 1) > 0 THEN
				RETURN NEXT (uuid, log_ids)::RECORD;
			EXIT;
		END IF;
		
		IF test_time_dis_constrain(pre_log.x, pre_log.y, cur_log.x, cur_log.y, pre_log.log_time, now_log.log_time) THEN
			array_append(log_ids, cur_log.id);
			pre_log := cur_log;
		ELSE THEN
			RETURN NEXT (uuid, log_ids)::RECORD;
			log_ids := ARRAY[cur_log.id];
			pre_log := cur_log;
		END IF;

		
		-- exit when no more row to fetch
		

		
	END LOOP;
  
   -- Close the cursor
   CLOSE cur_films;
 
   RETURN titles;
END; $$
LANGUAGE plpgsql;