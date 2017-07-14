# -*- encoding: utf-8
from query import get_tracks, get_closest_points
import math
import networkx as nx
# 标准正态分布的概率密度函数
def normal_distribution(x, u = 0.0, sigma = 20.0):    
    return (1.0 / ( ((2 * math.pi)  ** 0.5) * sigma)) * math.exp( -( (x-u)**2 / (2 * sigma**2) ) )


def construct_graph(log_ids, closest_points, log_closest_dict):

    g = nx.Graph()

    for log_id in log_ids:
        closest_idxs = log_closest_dict[log_id]
        for closest_idx in closest_idxs
            g.add_node(closest_idx)


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