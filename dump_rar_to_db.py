# -*- encoding: utf-8 -*-
# author: haoming
# 将*.rar文件导入数据库
#



import rarfile
import datetime
from insert import insert_logs
import time
from multiprocessing import Pool, cpu_count

rar_path_base = r'C:\tasks\road_network\shenzhen_2009_5_taxi_data\{0}.rar' #rar文件的路径
rarfile.UNRAR_TOOL = r'C:\app\unrar\UnRAR.exe' # unrar.exe的路径 






def insert_a_log_file_to_db(f):
    ''' insert a log file which storage one hour's gps log to db'''
    gps_logs_of_a_hour = [] 
    for ln in f:
        try:
            line = ln.strip()
            items = line[:-1].split(',')
            log = float(items[4])
            lat = float(items[5])
            if log < (108-0.5) or log > (114 + 0.5) or lat < (0+0.5) or (lat > 84+0.5):
                # utm49n 的bound是 log: 108-114 lat: 0-84 ,考虑了误差，将范围扩大0.5
                continue
            log_time = datetime.datetime.strptime(items[0] + items[1], '%Y%m%d%H%M%S')    
            unknown_1 = items[2]
            car_id = items[3]
            

            velocity = int(items[6])
            direction = int(items[7])
            on_service = bool(int(items[8]))
            is_valid = bool(int(items[9]))
            a_gps_log = (log_time, unknown_1, car_id, velocity, direction, on_service, is_valid, log, lat)
            gps_logs_of_a_hour.append(a_gps_log)
        except Exception as error:
            print('error: {0} is ommit'.format(error))
    print('{0}'.format(len(gps_logs_of_a_hour)))
    insert_logs(gps_logs_of_a_hour)


def insert_worker(rar_path):
    print(rar_path)

    begin_tick = time.time()

    with rarfile.RarFile(rar_path) as rf:
        for f in rf.infolist():
            # print f.filename
            if f.filename.split('/')[-1] == '7.txt' or f.filename.split('/')[-1] == '8.txt':
                print(f.filename)
                with rf.open(f.filename) as f:                   
                    insert_a_log_file_to_db(f)
    print('{0} s for insert {1}'.format(time.time() - begin_tick, rar_path))
    return cpu
    
def insert_callback(cpu):
    print("End of worker: " + str(cpu))











if __name__ == '__main__':
    pool = Pool(processes=cpu_count())
    for i in range(1, 32):
        rar_path = rar_path_base.format(i)
        pool.apply_async(insert_worker, (rar_path,), callback=insert_callback)
    pool.close()
    pool.join()