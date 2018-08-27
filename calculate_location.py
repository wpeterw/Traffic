from scipy import spatial
import requests

url = 'https://api.buienradar.nl/data/public/2.0/jsonfeed'
json_data = requests.get(url).json()
station_coordinates = []


def find_nearest_station(traffic_lat, traffic_lon):
    for i in range(len(json_data['actual']['stationmeasurements'])):
        lat = json_data['actual']['stationmeasurements'][i]['lat']
        lon = json_data['actual']['stationmeasurements'][i]['lon']
        location = (lat, lon)
        station_coordinates.append(location)

    tree = spatial.KDTree(station_coordinates)
    station = tree.query([(traffic_lat, traffic_lon)])[1]
    return station
