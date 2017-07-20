-- 安装dblink
CREATE EXTENSION dblink;

--  
-- @param begin_vid {{integer}} 顶点id
-- 计算所有到begin_vid直线距离小于5000m的顶点和begin_vid之间的dijkstra距离

-- 这样设计函数的原因是：
-- 1, prg_dijkstraCost函数的 many to many 接口,处理begin_vids和end_vids之间的两两组合而不是一一对应。所以没有办法对所有begin_vid进行计算。
-- 2, 对于每个begin_vid调用一次pgr_dijkstarCost函数，会在内存中重复加载路网数据，对性能有影响。 
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


-- 计算路网上任何两个直线距离小于5000米的顶点之间的dijkstra距离
-- 使用db_link模拟autonomous transaction来避免在一个事务的内存中保留过多的记录以致于内存耗尽
-- 使用db_link对性能大概有2倍影响，估计是建立连接花费了过多的时间

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
 		PERFORM dblink_connect('dblink_trans','dbname=road_gps port=5432 user=postgres password=123456');
 		PERFORM dblink('dblink_trans','INSERT INTO path_cost SELECT * FROM get_pair_distance(' || i || ');');
 		PERFORM dblink('dblink_trans','COMMIT;');
 		PERFORM dblink_disconnect('dblink_trans'); 
						
		--i := i+1;
		--RAISE NOTICE 'Counter: %', i;					
		--INSERT INTO path_cost SELECT * FROM get_pair_distance(i);
		
		
	END LOOP;
END;
$$
LANGUAGE 'plpgsql';


-- 1. create table
DROP TABLE path_cost;
CREATE TABLE path_cost
(
	start_vid BIGINT,
	end_vid BIGINT,
	agg_dis FLOAT
)


-- 2. invoke the function
SELECT calculate_all_dis();
-- 3. cost: 19:48 minutes

-- 4 create index
CREATE INDEX path_cost_idx ON path_cost(start_vid, end_vid);
-- 5 cluster table
CLUSTER path_cost USING path_cost_idx;
-- 6 vaccum and analyze
VACUUM ANALYZE path_cost;

-- 7 validate 12829162条
SELECT COUNT(*) FROM path_cost;

