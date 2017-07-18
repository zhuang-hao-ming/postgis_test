

CREATE OR REPLACE FUNCTION get_pair_distance (begin_vid INTEGER)
RETURNS TABLE (
	start_vid BIGINT,
	end_vid BIGINT,
	agg_dis float
)
AS 
$$
BEGIN
RETURN QUERY SELECT * FROM pgr_dijkstraCost('
		    SELECT 
				gid  AS id,
				source,
				target,
				cost as cost,
				cost as reverse_cost
			FROM
				shenzhen_network ORDER BY gid',
		    begin_vid,
		    (SELECT array_agg(b.id) FROM shenzhen_network_vertices_pgr a, shenzhen_network_vertices_pgr b WHERE ST_DWithin(a.the_geom, b.the_geom, 5000) AND a.id = begin_vid),
		    directed := false); 
END;
$$
LANGUAGE 'plpgsql';

SELECT * FROM get_pair_distance(2);




CREATE OR REPLACE FUNCTION calculate_all_dis(n INTEGER DEFAULT 16697)
RETURNS VOID 
AS
$$
DECLARE
	i INTEGER := 0;
	
BEGIN
	WHILE i <= n LOOP
		i := i+1;
		RAISE NOTICE 'Counter: %', i;
		
				
		INSERT INTO path_cost SELECT * FROM get_pair_distance(i);
		
		
	END LOOP;
END;
$$
LANGUAGE 'plpgsql';




CREATE TABLE path_cost
(
	start_vid BIGINT,
	end_vid BIGINT,
	agg_dis FLOAT
)

SELECT calculate_all_dis();

