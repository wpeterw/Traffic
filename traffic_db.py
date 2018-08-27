import sqlite3
traffic_table = 'traffic.db'

db = sqlite3.connect(traffic_table)

cursor = db.cursor()


def insert_traffic_data(input_list):
    create_table = 'CREATE TABLE IF NOT EXISTS traffic_table ' \
          '(msg_nr VARCHAR PRIMARY KEY, ' \
          'road VARCHAR,' \
          'category VARCHAR,' \
          'location_from_lat TEXT,' \
          'location_from_lon TEXT,' \
          'location_to_lat TEXT,' \
          'location_to_lon TEXT,' \
          'location_from VARCHAR,' \
          'location_to VARCHAR,'\
          'location_text VARCHAR,'\
          'reason VARCHAR,' \
          'seg_start VARCHAR,' \
          'seg_end VARCHAR,' \
          'description_text VARCHAR,' \
          'start_dat DATETIME,' \
          'end_dat DATETIME,' \
          'delay_num FLOAT,' \
          'distance FLOAT,' \
          'conditions VARCHAR,' \
          'rain_fall_last_hour FLOAT,' \
          'temperature FLOAT,' \
          'creation_date TEXT)' \

    cursor.execute(create_table)
    db.commit()
    sql = 'INSERT OR IGNORE INTO traffic_table(msg_nr, road, category, location_from_lat, location_from_lon, ' \
          'location_to_lat, location_to_lon, location_from, location_to, location_text, reason, seg_start, seg_end, ' \
          'description_text, start_dat, end_dat, delay_num, distance, conditions, rain_fall_last_hour, temperature, ' \
          'creation_date)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '
    cursor.executemany(sql, input_list)
    db.commit()


def select_all_from_db():
    cursor.execute('SELECT count(*) FROM traffic_table')
    rows = cursor.fetchall()
    return rows
