import psycopg2
from config import config



def get_logs(limit=10, offset=0):
    sql = '''
            SELECT id,log_time, car_id, direction AS v, ST_X(geom) AS x, ST_Y(geom) AS y  FROM gps_log_valid WHERE log_time::time between '07:30:00' AND '08:30:00' ORDER BY car_id, log_time LIMIT %s OFFSET %s;
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
            SELECT * FROM tracks WHERE array_length(points, 1) > 5 LIMIT 1;
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
        gps.direction AS v
	
    FROM 
        road r, 
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
