from django.contrib.gis.geos import GEOSGeometry
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import Parse
from joblib import delayed, Parallel
from datetime import datetime
import pandas as pd
import itertools
import psycopg2
import argparse
import zipfile
import time
import vaex
import os
import re


dir = os.path.dirname(__file__)
outdir = os.path.join(dir, "output")
gtfs_records_zip = os.path.join(dir, "data", "GtfsRRecords.zip")
gtfs_csv_zip = os.path.join(outdir, "gtfsr_csv.zip")
gtfs_final_csv_path = os.path.join(outdir, "gtfsr.csv")
gtfs_processed_csv_path = os.path.join(outdir, "gtfsr_processed.csv")

all_cols = [
    "trip_id",
    "start_date",
    "start_time",
    "stop_sequence",
    "departure",
    "arrival",
    "timestamp",
    "stop_id",
    "lon",
    "lat",
]

entity_cols = all_cols[:8]


# lets return a iterable that can be chunked
def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


# Template Function: connect to database and run query.
def run_query(query: str = ""):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(host="localhost", port="25432", database="gis", user="docker", password="docker")

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


# return a dataframe which includes all the stops id, lat, lon
def get_stops_df():
    query = """select stop_id, point from stop;"""
    res = run_query(query)

    stop_data = []
    for s in res:
        id, coords = s[0], GEOSGeometry(s[1]).coords
        lon, lat = coords[1], coords[0]

        stop_data.append([id, lon, lat])

    return pd.DataFrame(stop_data, columns=["stop_id", "lon", "lat"])


# realtime trip_id can be quite unstable,
# this functions attempts to alleviate it by searching and replacing
# observed anomalies.
def find_trip_regex(trip_list, trip_id):
    if not type(trip_id) == str:
        return None

    tokens = trip_id.split(".")
    if not len(tokens) == 5:
        return None

    route_id = tokens[2].split("-")

    if route_id[2] in ["ga2", "gad"]:
        route_id[2] = "ga[2|d]"
        tokens[2] = "-".join(route_id)
    elif route_id[2] in ["d12", "b12"]:
        route_id[2] = "[b|d]12"
        tokens[2] = "-".join(route_id)

    tokens[3] = "*"

    reg = ".".join(tokens)

    r = re.compile(reg)
    matched_list = list(filter(r.match, trip_list))

    if len(matched_list) > 0:
        return matched_list[0]
    else:
        return None


# splits from the main processing in order to allow multiple threads and cores
# for performance reasons
def multi_compute(i, data, trip_id_list):
    feed = gtfs_realtime_pb2.FeedMessage()
    entity_data = []

    try:
        Parse(data, feed)
    except:
        print("{}.json is a bad file, continue".format(i))
        return

    # get feed timestamp and iterate through all the entities
    timestamp = datetime.fromtimestamp(feed.header.timestamp)
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip_id = find_trip_regex(trip_id_list, entity.trip_update.trip.trip_id)

            # if the trip id exists in our database when can then continue processing
            if not trip_id == None:
                trip = entity.trip_update.trip
                stop_time_update = entity.trip_update.stop_time_update

                # for every stop_time_update we append add the fields needed
                for s in stop_time_update:
                    arr = s.arrival.delay if s.HasField("arrival") else 0
                    entity_data.append(
                        [
                            trip_id,
                            trip.start_date,
                            trip.start_time,
                            s.stop_sequence,
                            s.departure.delay,
                            arr,
                            timestamp,
                            s.stop_id,
                        ]
                    )

    # only if we have an existing trip in the feed we can produce a csv file
    if len(entity_data) > 0:
        # return the dataframe
        return pd.DataFrame(
            entity_data,
            columns=entity_cols,
        )


# here we split the data into smaller chunks by getting rid
# of what we dont need, this is done by only including the trips where
# we have a match in our database and disregarding the rest
def process_gtfsr_to_csv(chunk_size: int = 200, test: bool = False):
    start = time.time()

    query = """select trip_id from trip;"""
    trip_id_list = [id[0] for id in run_query(query)]

    # create empty zipfile
    zipfile.ZipFile(gtfs_csv_zip, "w").close()

    # read from the gtfs records
    with zipfile.ZipFile(gtfs_records_zip, "r") as zip:
        dirs = zip.namelist()
        dirs_len = len(dirs)

        curr_i = 0
        for chunk in chunked_iterable(dirs, size=chunk_size):

            delayed_func = [
                delayed(multi_compute)(curr_i + i, zip.read(dir), trip_id_list) for i, dir in enumerate(chunk)
            ]
            parallel_pool = Parallel(n_jobs=8)

            res = parallel_pool(delayed_func)

            # create df to store chunks
            gtfsr_df = pd.DataFrame()
            gtfsr_df = gtfsr_df.fillna(0)
            for r in res:
                if not type(r) == None:
                    gtfsr_df = pd.concat([gtfsr_df, r])

            curr_i += len(chunk)

            # here we drop any duplicates
            if len(gtfsr_df) > 0:
                gtfsr_df.columns = entity_cols
                gtfsr_df = gtfsr_df.sort_values(["arrival", "departure"], ascending=[True, True]).drop_duplicates(
                    subset=entity_cols[:6], keep="last"
                )

                # append csv to zip
                with zipfile.ZipFile(gtfs_csv_zip, "a") as zf:
                    zf.writestr(
                        "{}.csv".format(curr_i),
                        gtfsr_df.to_csv(header=False, index=False),
                        compress_type=zipfile.ZIP_DEFLATED,
                    )

            # friendly printing to update user
            print("{}/{}".format(curr_i, dirs_len), "time: {}s".format(round(time.time() - start)))

            if test == True:
                return

    print("finished processing")
    return


# here we combine all the zips from the csv into a dataframe and export to one csv file
def combine_csv():
    start = time.time()

    # read from the gtfs records
    with zipfile.ZipFile(gtfs_csv_zip, "r") as zip:
        dirs = zip.namelist()

        # merge all the csv's in the zip file
        combined_csv = pd.concat([pd.read_csv(zip.open(f), header=None) for f in dirs])
        combined_csv.columns = entity_cols

        # drop any duplicates excluding timestamp
        combined_csv = combined_csv.sort_values(["arrival", "departure"], ascending=[True, True]).drop_duplicates(
            subset=entity_cols[:6], keep="last"
        )

        # convert to csv
        combined_csv.to_csv(gtfs_final_csv_path, index=False, header=True)
        print("finished combining the zip files, time: {}".format(round(time.time() - start)))

        if os.path.exists(gtfs_final_csv_path + ".hdf5"):
            os.remove(gtfs_final_csv_path + ".hdf5")

        if not os.path.exists(gtfs_final_csv_path + ".hdf5"):
            vaex.from_csv(gtfs_final_csv_path, convert=True, copy_index=False, chunk_size=1000000)
            print("finished exporting converting to hdf5, time: {}".format(round(time.time() - start)))
    return


def process_data():
    start = time.time()

    stop_df = get_stops_df()

    df = pd.read_csv(gtfs_final_csv_path)

    # merge the entity stop_id data with the stop lat lon from database
    df = pd.merge(df, stop_df, on=["stop_id"])
    df.columns = all_cols

    df.to_csv(gtfs_processed_csv_path, index=False, header=True)

    print("finished processing data, {}".format(time.time() - start))


def extract_argv():
    print("started extraction process")
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    # execute only if run as a script
    process_gtfsr_to_csv()
    combine_csv()

    # remove the generated zip file at the end
    os.remove(gtfs_csv_zip)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--extract", help="extracts a zip of gtfsr records to a csv", action="store_true")
    parser.add_argument("--process", help="processes the created csv with the correct data format", action="store_true")
    parser.add_argument("--model", help="creates a prediction model using processed gtfsr data", action="store_true")
    parser.add_argument("--all", help="end2end extract process and create model", action="store_true")
    parser.add_argument("--test", help="export data to a diff file for testing", action="store_true")

    args = parser.parse_args()

    # testing  environment for
    if args.test and args.extract:
        gtfs_csv_zip = os.path.join(outdir, "gtfsr_csv_test.zip")
        gtfs_final_csv_path = os.path.join(outdir, "gtfsr_test.csv")

        process_gtfsr_to_csv(100, True)
        combine_csv()

    elif args.extract:
        extract_argv()

    if args.process:
        process_data()

    if args.model:
        print("creating model... not really")

    if args.all:
        extract_argv()
        process_data()
        print("model")
