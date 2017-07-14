import psycopg2
from config import config



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

def get_tracks():
    sql = '''
            SELECT * FROM tracks WHERE array_length(points, 1) > 10 AND id = 15 LIMIT 1;
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

def get_closest_points(log_ids):
    sql ='''
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
        gps.id in %s AND
        ST_DWithin(gps.geom, r.geom,  30);
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