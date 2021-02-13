import os
import zipfile
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import Parse
from django.contrib.gis.geos import GEOSGeometry
from pathlib import Path
import numpy as np
import pandas as pd
import psycopg2
import time

__file__ = Path().cwd()
gtfs_records_zip = os.path.join(__file__, 'GtfsRRecords.zip')
gtfs_csv_zip = os.path.join(__file__, 'gtfsr_csv.zip')


# Template Function: connect to database and run query.
def run_query(query: str = ''):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(
            host="localhost",
            port='25432',
            database="gis",
            user="docker",
            password="docker"
        )

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        cur.execute(query)
        data = cur.fetchall()

        # close the communication with the PostgreSQL
        cur.close()
        return data
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_stops_df():
    query = """select stop_id, point from stop;"""
    res = run_query(query)

    stop_data = []
    for s in res:
        id, coords = s[0], GEOSGeometry(s[1]).coords
        lon, lat = np.radians(coords[1]), np.radians(coords[0])

        stop_data.append([id, lon, lat])

    return pd.DataFrame(stop_data, columns=['stop_id', 'lon', 'lat'])


# here we split the data into smaller chunks by getting rid
# of what we dont need, this is done by only including the trips where
# we have a match in our database and disregarding the rest
def process_gtfsr_to_csv():
    start = time.time()

    query = """select trip_id from trip;"""
    trip_id_list = [id[0].replace('-d12-', '-b12-', 1)
                    for id in run_query(query)]

    stop_df = get_stops_df()

    # write to a new records file which we can then use to process data faster
    with zipfile.ZipFile(gtfs_csv_zip, 'w') as zf:

        # read from the gtfs records
        with zipfile.ZipFile(gtfs_records_zip, 'r') as zip:
            dirs = zip.namelist()
            dirs_len = len(dirs)

            for i in range(len(dirs)):
                feed = gtfs_realtime_pb2.FeedMessage()
                entity_data = []

                try:
                    realtime_data = zip.read(dirs[i])
                    Parse(realtime_data, feed)
                except:
                    print('{}.json is a bad file, continue'.format(i))
                    continue

                for entity in feed.entity:
                    if entity.HasField('trip_update'):
                        trip_id = entity.trip_update.trip.trip_id

                        if trip_id in trip_id_list:
                            trip = entity.trip_update.trip
                            stop_time_update = entity.trip_update.stop_time_update

                            for s in stop_time_update:
                                arr = s.arrival.delay if s.HasField(
                                    'arrival') else 0
                                entity_data.append(
                                    [trip.trip_id, trip.start_date, trip.start_time, s.stop_sequence, s.departure.delay, s.stop_id, arr])

                if i % 100 == 0:
                    print('{}/{}'.format(i, dirs_len),
                          'time: {}s'.format(round(time.time() - start)))

                if len(entity_data) > 0:
                    # create the entity
                    entity_df = pd.DataFrame(entity_data, columns=[
                                             'trip_id', 'start_date', 'start_time', 'stop_sequence', 'departure', 'stop_id', 'arrival'])
                    df = pd.merge(entity_df, stop_df, on=['stop_id'])
                    del df['stop_id']

                    zf.writestr("{}.csv".format(i), df.to_csv(header=False, index=False),
                                compress_type=zipfile.ZIP_DEFLATED)

    print('finished processing')
    return


def combine_csv():
    start = time.time()
    columns = ['trip_id', 'start_date', 'start_time',
               'stop_sequence', 'departure', 'arrival', 'lon', 'lat']

    # read from the gtfs records
    with zipfile.ZipFile(gtfs_csv_zip, 'r') as zip:
        dirs = zip.namelist()

        combined_csv = pd.concat(
            [pd.read_csv(zip.open(f), header=None) for f in dirs])
        combined_csv.columns = columns
        combined_csv.to_csv('gtfsr_combined_csv.csv', index=False, header=True)

    print('finished cobining the zip files, time: {}'.format(
        round(time.time() - start)))
    return


if __name__ == "__main__":
    # execute only if run as a script
    process_gtfsr_to_csv()
    combine_csv()
