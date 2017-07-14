# -*- encoding: utf-8
from query import get_tracks, get_closest_points
import math
import networkx as nx
import matplotlib.pyplot as plt
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
    log_x, log_y, p_x, p_y, line_id, log_id, v = closest_point

    dis = euclidan_dis(log_x, log_y, p_x, p_y)

    return normal_distribution(dis)

# 欧氏距离
def euclidan_dis(x1, y1, x2, y2):
    return ( (x1 - x2) ** 2 + (y1 - y2) ** 2 ) ** 0.5





def get_path_distance(pre_closest_point, now_closest_points)
    pre_log_x, pre_log_y, pre_p_x, pre_p_y, pre_line_id, pre_log_id, v = pre_closest_point
    now_log_x, now_log_y, now_p_x, now_p_y, now_line_id, now_log_id, v = now_closest_points

    if pre_line_id == now_line_id: # 共线
        x1 = now_log_x - pre_log_x
        y1 = now_log_y - pre_log_y
        x2 = now_p_x - pre_p_x
        y2 = now_p_y - pre_p_y
        
        if is_same_direction(x1, y1, x2, y2): # 共向

            return 0 # 返回道路上的距离


        else: # 反向
            if 1： # 道路双向
                return 9999 # 认为这是一种错误的情况， 返回一个大值
            else: # 道路单向
                return 9999 # 认为这是一种错误的情况， 返回一个大值


        pass
    else: #不共线
        if 1: # p1双向
            if 1: # p2双向
                
                
            else:
                pass
        else: # p1单向
            if 1: # p2双向
                pass
            else: # p2单向
                pass



# transimission probability
def get_transmission_probability(pre_closest_point, now_closest_points):
    pass




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

                    g.add_edge(idx, closest_idx)

        pre_layer_idx = now_layer_idx
    
    


    
                    
    # 观察概率
    
    





def main():

    tracks = get_tracks() #获得轨迹
    for track in tracks: #遍历轨迹
        
        log_closest_dict = {}
        
        for log_id in track[1]: #遍历轨迹的gps log id
            log_closest_dict[int(log_id)] = []
        
        
        closest_points = get_closest_points(tuple(track[1])) # get gps log id's closest point in raod network
        
        for idx, point in enumerate(closest_points):
            log_x, log_y, p_x, p_y, line_id, log_id, v = point                                    
            log_closest_dict[int(log_id)].append(idx)
        
        print(log_closest_dict)

        construct_graph(track[1] , closest_points, log_closest_dict)

        
        
        
        


if __name__ == '__main__':
    main()