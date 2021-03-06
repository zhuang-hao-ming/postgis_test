import psycopg2
import config
import datetime



def insert_track(tracks):

    sql = '''
    INSERT INTO tracks(points) VALUES (%s);
    '''
    conn = None
    try:
        params = config.config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, tracks)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_match(match_list, closest_points, track_id):

    sql = '''
    INSERT INTO match_track(track_id, geom) 
    VALUES
	(%s, ST_GeomFromText(%s, 32649))
    '''

    wkt_multipoint_base = 'MULTIPOINT({0})'
    point_datas = []
    for idx in match_list:
        now_log_x, now_log_y, now_p_x, now_p_y, now_line_id, now_gps_log_id, now_v,  now_source, now_target, now_length, now_fraction = closest_points[idx]
        point_data = '{0} {1}'.format(now_p_x, now_p_y)
        point_datas.append(point_data)

    wkt_multipoint = wkt_multipoint_base.format(','.join(point_datas))

   

    conn = None
    try:
        params = config.config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (track_id, wkt_multipoint))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


















def insert_logs(gps_logs):
    # sql = '''
    # INSERT INTO gps_log(log_time, unknown_1, car_id, velocity, direction, on_service, is_valid, geom)
    # VALUES (%s, %s, %s, %s, %s, %s, %s, ST_GeomFromText('POINT(%s %s)', 4326));

    # '''
    sql = '''
    INSERT INTO gps_log(log_time, unknown_1, car_id, velocity, direction, on_service, is_valid, geom)
    VALUES (%s, %s, %s, %s, %s, %s, %s, ST_Transform(ST_GeomFromText('POINT(%s %s)', 4326), 32649));
    '''
    

    conn = None
    try:
        params = config.config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, gps_logs)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    log_time = datetime.datetime.strptime('20090501' + '005754', '%Y%m%d%H%M%S')    
    unknown_1 = 'H'
    car_id = '13013814358'
    log = 114.076150
    lat = 22.543683
    velocity = 42
    direction = 8
    on_service = False
    is_valid = True

    a_gps_log = (log_time, unknown_1, car_id, velocity, direction, on_service, is_valid, log, lat)

    #insert_logs([a_gps_log])
    a_track = ([21312,32114],)
    insert_track([a_track])