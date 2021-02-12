import os
import zipfile
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import Parse, MessageToJson
from datetime import datetime
from pathlib import Path
import psycopg2


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


# here we split the data into smaller chunks by getting rid
# of what we dont need
def process_gtfsr():
    __file__ = Path().cwd()

    gtfsRecords = os.path.join(__file__, 'GtfsRRecords.zip')

    query = """select trip_id from trip;"""
    trip_id_list = [id[0] for id in run_query(query)]

    # write to a new records file which we can then use to process data faster
    with zipfile.ZipFile(os.path.join(__file__, 'trimmed_gtfsr.zip'), 'w') as zf:

        # read from the gtfs records
        with zipfile.ZipFile(gtfsRecords, 'r') as zip:
            dirs = zip.namelist()
            dirs_len = len(dirs)

            for i in range(len(dirs)):
                realtime_data = zip.read(dirs[i])

                feed = gtfs_realtime_pb2.FeedMessage()
                Parse(realtime_data, feed)

                timestamp = datetime.fromtimestamp(feed.header.timestamp)

                entity_list = []
                for entity in feed.entity:
                    if entity.HasField('trip_update'):
                        trip_id = str(entity.trip_update.trip.trip_id).replace(
                            '-b12-', '-d12-', 1)

                        if trip_id in trip_id_list:
                            entity_list.append(entity)

                if i % 10 == 0:
                    print(i, '/', dirs_len, ', ', timestamp)

                if len(entity_list) > 0:
                    del feed.entity[:]
                    feed.entity.extend(entity_list)

                    zf.writestr("{}.json".format(i), MessageToJson(feed),
                                compress_type=zipfile.ZIP_DEFLATED)

    print('finished processing')
    return


if __name__ == "__main__":
    # execute only if run as a script
    process_gtfsr()
