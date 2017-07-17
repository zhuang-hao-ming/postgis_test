# -*- encoding: utf-8

from query import get_tracks, get_closest_points, get_distance_in_linestring, get_path_cost, get_distance_to_start
from insert import insert_match
import math
import networkx as nx
import matplotlib.pyplot as plt
import time

# 标准正态分布的概率密度函数
def normal_distribution(x, u = 0.0, sigma = 20.0):    
    return (1.0 / ( ((2 * math.pi)  ** 0.5) * sigma)) * math.exp( -( (x-u)**2 / (2 * sigma**2) ) )

# 判断向量(x1,y1)和（x2,y2）是否共向
def is_same_direction(x1, y1, x2, y2):
    if x1*x2 + y1*y2 > 0:
        return True
    else:
        return False

# observation probability
def get_observation_prob(closest_point):
    log_x, log_y, p_x, p_y, line_id, log_id, v, source, target = closest_point

    dis = euclidan_dis(log_x, log_y, p_x, p_y)

    return normal_distribution(dis)

# 欧氏距离
def euclidan_dis(x1, y1, x2, y2):
    return ( (x1 - x2) ** 2 + (y1 - y2) ** 2 ) ** 0.5





def get_path_distance(pre_closest_point, now_closest_point):
    pre_log_x, pre_log_y, pre_p_x, pre_p_y, pre_line_id, pre_log_id, pre_v, pre_source, pre_target = pre_closest_point
    now_log_x, now_log_y, now_p_x, now_p_y, now_line_id, now_log_id, now_v, now_source, now_target = now_closest_point

    if pre_line_id == now_line_id: # 共线
        x1 = now_log_x - pre_log_x
        y1 = now_log_y - pre_log_y
        x2 = now_p_x - pre_p_x
        y2 = now_p_y - pre_p_y
        
        if is_same_direction(x1, y1, x2, y2): # 共向

            
            path_dis = get_distance_in_linestring(pre_p_x, pre_p_y, now_p_x, now_p_y, now_line_id)[0]
            return path_dis # 返回道路上的距离

        else: # 反向
            if 1: # 道路双向
                return 9999 # 认为这是一种错误的情况， 返回一个大值
            else: # 道路单向
                return 9999 # 认为这是一种错误的情况， 返回一个大值


        pass
    else: #不共线
        if 1: # p1双向
            if 1: # p2双向

                pre_proportion, pre_len = get_distance_to_start(pre_p_x, pre_p_y, pre_line_id) # p1到路径起点长度的比例， 路径的长度
                now_proportion, now_len = get_distance_to_start(now_p_x, now_p_y, now_line_id) # p2到路径起点长度的比例， 路径的长度

                path_dis_dict = {
                    pre_source: {},
                    pre_target: {}
                }

                source_ids = [pre_source, pre_target]
                target_ids = [now_source, now_target]
                

                rows = get_path_cost(source_ids, target_ids) # 得到p1所在路径的起点终点，到p2所在路径的起点终点的dijkstra距离

                for row in rows:
                    path_dis_dict[row[0]][row[1]] = row[2]
                
                min_path_dis = 999999999
                for id_x, key_x in enumerate(source_ids):
                    for id_y, key_y in enumerate(target_ids):
                        
                            routing_dis = path_dis_dict[key_x].get(key_y, -100) 
                            if routing_dis == -100: # 不存在可能为0或者为无穷大
                                if key_x in target_ids:
                                    routing_dis = 0
                                else:
                                    routing_dis = 999999999
                            if id_x == 0: # 从p1的source出发                    
                                routing_dis += pre_len * pre_proportion 
                            elif id_x ==1: # 从p1的target出发                        
                                routing_dis += pre_len * (1.0 - pre_proportion)
                            if id_y == 0: # 到达p2的source
                                routing_dis += now_len * now_proportion
                            elif id_y == 1: # 到达p2的target
                                routing_dis += now_len * (1.0 - now_proportion)

                            if routing_dis < min_path_dis:
                                min_path_dis = routing_dis


                return min_path_dis
            else: #p2单
                pass
        else: # p1单向
            if 1: # p2双向
                pass
            else: # p2单向
                pass



# transimission probability
def get_transmission_probability(pre_closest_point, now_closest_point):
    p_path_dis = get_path_distance(pre_closest_point, now_closest_point) # 两个匹配点的路径距离

    pre_log_x, pre_log_y, pre_p_x, pre_p_y, pre_line_id, pre_log_id, pre_v, pre_source, pre_target = pre_closest_point
    now_log_x, now_log_y, now_p_x, now_p_y, now_line_id, now_log_id, now_v, now_source, now_target = now_closest_point

    log_dis = euclidan_dis(pre_log_x, pre_log_y, now_log_x, now_log_y) # 两个gps点的直线距离

    return log_dis / (p_path_dis+0.0000001) # 转移概率



#
# @param log_ids {{list}} log_id list
# @param closest_points {{list}} closest point list
# @param log_closest_dict {{dict}} log_id: [closest_pnt_idx1, closest_pnt_idx2 ...]
#
def construct_graph(log_ids, closest_points, log_closest_dict):

    g = nx.Graph()
    pre_layer_idx = []
    for log_idx, log_id in enumerate(log_ids):
        closest_idxs = log_closest_dict[log_id]
        now_layer_idx = []
        for closest_idx in closest_idxs:
            now_layer_idx.append(closest_idx)


            observation_prob = get_observation_prob(closest_points[closest_idx])

            g.add_node(closest_idx, observation_prob=observation_prob)
            if log_idx == 0:
                continue
            else:
                for idx in pre_layer_idx:

                    transmission_prob = get_transmission_probability(closest_points[idx], closest_points[closest_idx])

                    g.add_edge(idx, closest_idx, transmission_prob=transmission_prob)

        pre_layer_idx = now_layer_idx
    
    return g


    
                    
def find_match_seqence(g, log_ids, log_closest_dict):
    f = {}
    pre = {}
    for idx in log_closest_dict[log_ids[0]]:
             
        f[idx] = g.node[idx]['observation_prob']        
    for layer_idx, log_id in enumerate(log_ids[1:]):
        for p_idx in log_closest_dict[log_id]:
            max_f = -99999999
            for p_p_idx in log_closest_dict[log_ids[layer_idx]]:
                alt = g.edge[p_p_idx][p_idx]['transmission_prob'] + f[p_p_idx]
                if alt > max_f:
                    max_f = alt
                    pre[p_idx] = p_p_idx
                f[p_idx] = max_f
    max_c = -99999999
    max_key = None
    for key, val in f.items():
        if val > max_c:
            max_key = key
            max_c = val
        else:
            continue
    r_list = []
    for i in range(1, len(log_ids)):
        r_list.append(max_key)
        max_key = pre[max_key]
    r_list.append(max_key)
    r_list.reverse()
    return r_list
    

    





def main():

    

    tracks = get_tracks() #获得轨迹
    begin_main = time.time()
    for track in tracks: #遍历轨迹
        
        begin_track = time.time()

        log_closest_dict = {}
        
        for log_id in track[1]: #遍历轨迹的gps log id
            log_closest_dict[int(log_id)] = []
        
        
        closest_points = get_closest_points(tuple(track[1])) # get gps log id's closest point in raod network
        
        for idx, point in enumerate(closest_points):
            log_x, log_y, p_x, p_y, line_id, log_id, v, source, target = point                                    
            log_closest_dict[int(log_id)].append(idx)
        
        

        g = construct_graph(track[1] , closest_points, log_closest_dict)
        
        match_list = find_match_seqence(g, track[1], log_closest_dict)
        
        insert_match(match_list, closest_points, track[0])
        
        print('track({0}): {1} time: {2} elapse: {3}'.format(track[0], len(track[1]), time.time() - begin_track, time.time() - begin_main))
        


if __name__ == '__main__':
    main()