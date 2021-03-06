# -*- encoding: utf-8
import psycopg2
from config import config

def get_nodes():
    sql = '''
        SELECT id FROM shenzhen_network_vertices_pgr;
    '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_edges():
    sql = '''
        SELECT source, target, cost FROM shenzhen_network;
        '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()  


def get_logs(limit=10, offset=0):
    sql = '''
            SELECT id,log_time, car_id, direction AS v, ST_X(geom) AS x, ST_Y(geom) AS y  FROM gps_log_valid ORDER BY car_id, log_time LIMIT %s OFFSET %s;
          '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql, (limit, offset))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_distance_rows(vids):
    sql = '''
        SELECT * FROM path_cost WHERE start_vid in %s AND end_vid in %s;
        '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql, (vids, vids))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_tracks():
    sql = '''
            SELECT * FROM tracks WHERE array_length(points, 1) > 15 LIMIT 1000;
          '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



def get_closest_points1(log_ids):

    sql ='''
   WITH closest_points AS
(
	SELECT 
		gps.geom as geom_log,
		r.geom_l as geom_line,
		r.source as source,
		r.target as target,
		r.gid as line_id,
		gps.id as log_id,
		gps.direction as v,
		ST_ClosestPoint(r.geom_l, gps.geom) as geom_closest,
		r.cost as length
	FROM
		shenzhen_network r, 
		gps_log_valid gps
	WHERE  
		gps.id in %s AND
		ST_DWithin(gps.geom, r.geom_l,  30)
)	
SELECT 
	ST_X(geom_log) AS log_x,
	ST_Y(geom_log) AS log_y,
	ST_X(geom_closest) AS p_x,
	ST_Y(geom_closest) AS p_y,
	line_id,
	log_id,
	v,
	source,
	target,	
	length,
	ST_LineLocatePoint(geom_line, geom_log) as fraction
FROM
	closest_points;
    '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql, (log_ids,))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()






def get_closest_points(track_id):
    sql ='''
    SELECT 
        log_x,
        log_y,
        p_x,
        p_y,
        v,
        gps_log_id,
        edge_id,
        fraction,
	    source,
        target
	
    FROM 
        points_of_interest
    WHERE 
        track_id = %s
    '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql, (track_id,))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_distance_in_linestring(pre_x, pre_y, now_x, now_y, line_id):
    sql = '''
            WITH
            data AS (
                SELECT ST_GeomFromText('POINT(%s %s)', 32649) pta,
                    ST_GeomFromText('POINT(%s %s)', 32649) ptb,
                    ST_GeometryN(geom, 1)::geometry(linestring, 32649) line FROM shenzhen_network WHERE gid = %s
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
    '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql, (pre_x, pre_y, now_x, now_y, line_id))
        row = cur.fetchone()
        cur.close()
        return row
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_distance_to_start(x, y, line_id):
    sql = '''
        WITH
        data AS (
        SELECT 
            ST_GeomFromText('POINT(%s %s)', 32649) pta,
            ST_GeometryN(geom, 1)::geometry(linestring, 32649) line FROM shenzhen_network WHERE gid=%s
        )
        SELECT  ST_LineLocatePoint(line, pta), ST_Length(line) FROM data;
    '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql, (x, y, line_id))
        row = cur.fetchone()
        cur.close()
        return row
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()    

def get_path_cost(pre_ids, now_ids):
    sql ='''
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
    '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql, (pre_ids,now_ids))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()