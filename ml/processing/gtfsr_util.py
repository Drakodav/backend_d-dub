from joblib import delayed, Parallel, load, parallel_backend
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import Parse
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import geopandas as gpd
import itertools
import psycopg2
import argparse
import zipfile
import time
import vaex
import vaex.ml
import os
import re


dir = os.path.dirname(__file__)
outdir = os.path.join(dir, "output")
gtfs_records_zip = os.path.join(dir, "data", "GtfsRRecords.zip")
gtfs_csv_zip = os.path.join(outdir, "gtfsr_csv.zip")
gtfs_final_csv_path = os.path.join(outdir, "gtfsr.csv")
gtfs_processed_csv_path = os.path.join(outdir, "gtfsr_processed.csv")
scats = os.path.join(dir, "output", "scats_model.json")
gtfsr_processing_temp = os.path.join(outdir, "processing_temp.hdf5")


entity_cols = [
    "trip_id",
    "start_date",
    "start_time",
    "stop_sequence",
    "departure",
    "arrival",
    "timestamp",
    "stop_id",
]


# return the time taken until now
def duration(start):
    return round(time.time() - start)


# parse a string to datetime
def get_dt(dt, format):
    return datetime.strptime(str(dt), format)


# lets return a iterable that can be chunked
def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


# return the dow for arrival time and departure time.
# since a trip that starts at 11.30pm can  have an arrival time of 00:01am
# therefore its the next day
def apply_dow(start_date, start_time, expected_time):
    date = get_dt(start_date, "%Y%m%d")
    if get_dt(start_time, "%H:%M:%S") > get_dt(expected_time, "%H:%M:%S"):
        return (date + timedelta(days=1)).weekday()
    return date.weekday()


# connect to the PostgreSQL server
def get_conn():
    return psycopg2.connect(host="localhost", port="25432", database="gis", user="docker", password="docker")


# Template Function: connect to database and run query.
def run_query(query: str = ""):
    """ Connect to the PostgreSQL database server """
    try:
        conn = get_conn()

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


# splits from the main processing of process_gtfsr_to_csv in order to
# allow multiple threads and cores for performance reasons
def multi_compute(i, data, trip_id_list):
    feed = gtfs_realtime_pb2.FeedMessage()
    entity_data = []

    try:
        Parse(data, feed)
    except:
        print("{}.json is a bad file, continue".format(i))
        return

    # get feed timestamp and iterate through all the entities
    timestamp = datetime.utcfromtimestamp(feed.header.timestamp)
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

            # write the data to csv
            if len(gtfsr_df) > 0:
                gtfsr_df.columns = entity_cols

                # append csv to zip
                with zipfile.ZipFile(gtfs_csv_zip, "a") as zf:
                    zf.writestr(
                        "{}.csv".format(curr_i),
                        gtfsr_df.to_csv(header=False, index=False),
                        compress_type=zipfile.ZIP_DEFLATED,
                    )

            # friendly printing to update user
            print("{}/{}".format(curr_i, dirs_len), "time: {}s".format(duration(start)))

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

        # convert to csv
        combined_csv.to_csv(gtfs_final_csv_path, index=False, header=True)
        print("finished combining the zip files, time: {}".format(duration(start)))

        if os.path.exists(gtfs_final_csv_path + ".hdf5"):
            os.remove(gtfs_final_csv_path + ".hdf5")

        if not os.path.exists(gtfs_final_csv_path + ".hdf5"):
            vaex.from_csv(gtfs_final_csv_path, convert=True, copy_index=False, chunk_size=1000000)
            print("finished exporting converting to hdf5, time: {}".format(duration(start)))
    return


# calculate the direction angle from point 1 to point 2
# in our case we use first stop and last stop lon lats
def direction_angle(theta_1, phi_1, theta_2, phi_2):
    dtheta = theta_2 - theta_1
    dphi = phi_2 - phi_1
    radians = np.arctan2(dtheta, dphi)
    return np.rad2deg(radians)


# get the stop time, stop and trip data for each trip
def get_stop_time_df(trip_id, conn):
    query = """
    select 
        stop_time.arrival_time, stop_time.departure_time,
        stop_time.stop_sequence, stop_time.shape_dist_traveled, 
        stop.stop_id, stop.point as geom,
        trip.direction, route.route_id
    from stop_time
    join stop on stop.id = stop_time.stop_id
    join trip on trip.id = stop_time.trip_id
    join route on trip.route_id = route.id
    where trip.trip_id = '{}'
    group by stop_time.id, stop.id, trip.id, route.id
    order by stop_sequence
    ;
    """.format(
        trip_id
    ).lstrip()

    gdf = gpd.read_postgis(query, conn())

    # convert the times to human readable format, !IMPORTANT! utcfromtimestamp returns the correct version
    gdf["arrival_time"] = gdf["arrival_time"].apply(lambda d: datetime.utcfromtimestamp(d).strftime("%H:%M:%S"))
    gdf["departure_time"] = gdf["departure_time"].apply(lambda d: datetime.utcfromtimestamp(d).strftime("%H:%M:%S"))

    # convert the geom to lat lon
    gdf["lat"] = gdf.apply(lambda row: row["geom"].y, axis=1)
    gdf["lon"] = gdf.apply(lambda row: row["geom"].x, axis=1)

    # find the direction angle of the trip
    gdf["direction_angle"] = direction_angle(gdf.iloc[0].lon, gdf.iloc[0].lat, gdf.iloc[-1].lon, gdf.iloc[-1].lat)

    # calculate the point distance between each stop and shape dist between them
    gdf["shape_dist_between"] = gdf.shape_dist_traveled - gdf.shape_dist_traveled.shift()

    gdf["trip_id"] = trip_id  # set the trip id, no need to fetch from db
    gdf["start_time"] = gdf["arrival_time"].iloc[0]  # set the start time to the first instance of arrival time
    gdf = gdf.fillna(0)  # first will always be NA, set to 0

    # return a new pandas df dropping the geom column
    return pd.DataFrame(gdf.drop(columns="geom"))


# create a df with stop data and export to hdf5
def add_stop_data(start):
    print("*** adding stop data***")

    df = pd.read_csv(gtfs_final_csv_path)  # read csv

    # dropping duplicates
    df = df.drop_duplicates(subset=entity_cols[:5])
    print("dropped duplicates, time: {}".format(duration(start)))

    # get all the stop_times for each trip in our realtime data
    trip_list = df["trip_id"].unique()
    delayed_funcs = [delayed(get_stop_time_df)(t_id, get_conn) for t_id in trip_list]

    parallel_pool = Parallel(n_jobs=8)
    res = parallel_pool(delayed_funcs)

    # stop times for each trip dataframe
    stop_time_trip_df = pd.concat(res)
    print("concat stop_time data, time: {}".format(duration(start)))

    df = df.merge(
        stop_time_trip_df,
        left_on=["trip_id", "stop_sequence", "stop_id", "start_time"],
        right_on=["trip_id", "stop_sequence", "stop_id", "start_time"],
    )
    print("merge stop_time & gtfsr data, time: {}".format(duration(start)))

    vaex.from_pandas(df).export_hdf5(gtfsr_processing_temp)  # convert to hdf5


def predict_traffic_from_scats(start):
    print("*** scats predictions ***")

    df = vaex.open(gtfsr_processing_temp)

    df["hour"] = df["arrival_time"].apply(lambda t: get_dt(t, "%H:%M:%S").hour)
    df["dow"] = df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])

    pca_coord = vaex.ml.PCA(features=["lat", "lon"], n_components=2, prefix="pca")
    df = pca_coord.fit_transform(df)

    cycl_transform_hour = vaex.ml.CycleTransformer(features=["hour"], n=24)
    df = cycl_transform_hour.fit_transform(df)

    cycl_transform_dow = vaex.ml.CycleTransformer(features=["dow"], n=7)
    df = cycl_transform_dow.fit_transform(df)

    with parallel_backend("threading"):
        # load the scats ml model
        scats_model = load(scats)

        # get the predictions from scats data
        df = scats_model.transform(df)
        print("made predictions, time: {}".format(duration(start)))
        print("exporting to csv...")

        df[df.get_column_names(virtual=False) + ["p_avg_vol"]].export_csv(gtfs_processed_csv_path)

        if os.path.exists(gtfs_processed_csv_path + ".hdf5"):
            os.remove(gtfs_processed_csv_path + ".hdf5")
        vaex.from_csv(gtfs_processed_csv_path, convert=True)

        os.remove(gtfsr_processing_temp)

        print("exported to csv, time: {}".format(duration(start)))

    return


def process_data():
    start = time.time()

    if not os.path.exists(gtfsr_processing_temp):
        add_stop_data(start)

    predict_traffic_from_scats(start)

    print("finished processing data, {}".format(duration(start)))


def extract():
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
        extract()

    if args.process:
        process_data()

    if args.model:
        # train_gtfsr()
        print("creating model... not really")

    if args.all:
        extract()
        process_data()
        print("model")
