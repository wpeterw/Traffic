import requests
import traffic_db as db
from scipy import spatial
import datetime
import time

traffic_url = 'https://www.anwb.nl/feeds/gethf'
weather_url = 'https://api.buienradar.nl/data/public/2.0/jsonfeed'
headers = {'User-agent': 'Mozilla/5.0'}

traffic_json_data = requests.get(traffic_url, headers=headers).json()
weather_json_data = requests.get(weather_url, headers=headers).json()

records = []
station_coordinates = []


def get_station_coordinates():
    for i in range(len(weather_json_data['actual']['stationmeasurements'])):
        lat = weather_json_data['actual']['stationmeasurements'][i]['lat']
        lon = weather_json_data['actual']['stationmeasurements'][i]['lon']
        location = (lat, lon)
        station_coordinates.append(location)
    return station_coordinates


def find_nearest_station(traffic_lat, traffic_lon, station_list):
    tree = spatial.KDTree(station_list)
    station = tree.query([(traffic_lat, traffic_lon)])[1]
    return station


def json_to_db():
    while True:
        station_list = get_station_coordinates()
        for category in ('trafficJams', 'roadWorks', 'radars'):
            for i in range(0, len(traffic_json_data['roadEntries'])):
                for n in range(0, len(traffic_json_data['roadEntries'][i]['events'][category])):
                    road = traffic_json_data['roadEntries'][i]['road']
                    msg_nr = traffic_json_data['roadEntries'][i]['events'][category][n]['msgNr']
                    location_from = traffic_json_data['roadEntries'][i]['events'][category][n]['from']
                    location_from_lat = traffic_json_data['roadEntries'][i]['events'][category][n]['fromLoc']['lat']
                    location_from_lon = traffic_json_data['roadEntries'][i]['events'][category][n]['fromLoc']['lon']
                    location_to_lat = traffic_json_data['roadEntries'][i]['events'][category][n]['toLoc']['lat']
                    location_to_lon = traffic_json_data['roadEntries'][i]['events'][category][n]['toLoc']['lon']
                    location_to = traffic_json_data['roadEntries'][i]['events'][category][n]['to']
                    seg_start = traffic_json_data['roadEntries'][i]['events'][category][n]['segStart']
                    seg_end = traffic_json_data['roadEntries'][i]['events'][category][n]['segEnd']
                    location_text = traffic_json_data['roadEntries'][i]['events'][category][n]['location']
                    reason = traffic_json_data['roadEntries'][i]['events'][category][n]['reason']
                    description_text = traffic_json_data['roadEntries'][i]['events'][category][n]['description']
                    try:
                        start_dat = traffic_json_data['roadEntries'][i]['events'][category][n]['start']
                        end_dat = traffic_json_data['roadEntries'][i]['events'][category][n]['stop']
                    except KeyError:
                        start_dat = ''
                        end_dat = ''
                    try:
                        delay_num = traffic_json_data['roadEntries'][i]['events'][category][n]['delay']
                        distance = traffic_json_data['roadEntries'][i]['events'][category][n]['distance']
                    except KeyError:
                        delay_num = ''
                        distance = ''

                    weather_station = find_nearest_station(location_from_lat, location_from_lon, station_list)[0]
                    conditions = weather_json_data['actual']['stationmeasurements'][weather_station]['weatherdescription']
                    rain_fall_last_hour = weather_json_data['actual']['stationmeasurements'][weather_station][
                        'rainFallLastHour']
                    temperature = weather_json_data['actual']['stationmeasurements'][weather_station]['temperature']
                    creation_date = datetime.datetime.now()

                    record = (msg_nr, road, category, location_from_lat, location_from_lon, location_to_lat,
                              location_to_lon, location_from, location_to, location_text, reason, seg_start,
                              seg_end, description_text, start_dat, end_dat, delay_num, distance, conditions,
                              rain_fall_last_hour, temperature, creation_date)
                    records.append(record)

         db.insert_traffic_data(records)
        time.sleep(60)

json_to_db()
result = db.select_all_from_db()
print(result[0][0])

