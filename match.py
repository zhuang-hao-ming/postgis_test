# -*- encoding: utf-8
from query import get_tracks, get_closest_points

def main():

    tracks = get_tracks() #获得轨迹
    for track in tracks: #遍历轨迹
        
        log_closest_dict = {}
        for log_id in track[1]: #遍历轨迹的gps log id
            log_closest_dict[log_id] = []
        
        closest_points = get_closest_points(tuple(track[1])) # get gps log id's closest point in raod network
        
        for point in closest_points:
            log_x, log_y, p_x, p_y, line_id, log_id, v = point                        
            log_closest_dict[log_id].append({
                'log_x': log_x,
                'log_y': log_y,
                'p_x': p_x,
                'p_y': p_y,
                'line_id': line_id,
                'v': v
            })
        
        


if __name__ == '__main__':
    main()