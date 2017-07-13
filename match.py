from query import get_tracks

def main():
    tracks = get_tracks()
    for track in tracks:
        print(track)


if __name__ == '__main__':
    main()