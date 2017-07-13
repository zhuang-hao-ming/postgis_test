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

