from __future__ import absolute_import, unicode_literals

from joblib import delayed, Parallel, load, parallel_backend
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import Parse
from datetime import datetime
import geopandas as gpd
import pandas as pd
import argparse
import zipfile
import vaex.ml
import time
import vaex
import sys
import os

from vaex.ml.sklearn import Predictor
import lightgbm
import xgboost as xgb

from ml.processing.util import (
    apply_dow,
    chunked_iterable,
    find_trip_regex,
    get_conn,
    get_dt,
    run_query,
    vaex_mjoin,
    is_delay,
    direction_angle,
)

dir = os.path.dirname(__file__)
outdir = os.path.join(dir, "output")
gtfs_records_zip = os.path.join(dir, "data", "GtfsRRecords.zip")
gtfs_csv_zip = os.path.join(outdir, "gtfsr_csv.zip")
gtfs_final_csv_path = os.path.join(outdir, "gtfsr.csv")
gtfs_final_hdf5_path = os.path.join(outdir, "gtfsr.csv.hdf5")
gtfs_processed_path = os.path.join(outdir, "gtfsr_processed.hdf5")
scats_model_path = os.path.join(outdir, "scats_model.json")
gtfsr_processing_temp = os.path.join(outdir, "processing_temp.hdf5")
gtfsr_historical_means_path = os.path.join(outdir, "gtfsr_historical_means.hdf5")
stop_time_data_path = os.path.join(outdir, "stop_time_data.hdf5")

start = time.time()

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
def duration():
    return round(time.time() - start)


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
def process_gtfsr_to_csv(chunk_size: int = 200):
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
            print(f"{curr_i}/{dirs_len}, time: {duration()}s")

    print("finished processing")
    return


# here we combine all the zips from the csv into a dataframe and export to one csv file
def combine_csv():
    # read from the gtfs records
    with zipfile.ZipFile(gtfs_csv_zip, "r") as zip:
        dirs = zip.namelist()

        # merge all the csv's in the zip file
        combined_csv = pd.concat([pd.read_csv(zip.open(f), header=None) for f in dirs])
        combined_csv.columns = entity_cols

        # dropping duplicates
        combined_csv = combined_csv.drop_duplicates(subset=entity_cols[:5])

        # convert to csv
        combined_csv.to_csv(gtfs_final_csv_path, index=False, header=True)
        print(f"finished combining the zip files, time: {duration()}")

        if os.path.exists(gtfs_final_hdf5_path):
            os.remove(gtfs_final_hdf5_path)

        vaex.from_csv(gtfs_final_csv_path, convert=True, copy_index=False, chunk_size=1000000)
        print(f"finished converting to hdf5, time: {duration()}")
    return


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
def create_stop_time_data():
    print("*** creating stop time data ***")

    df = vaex.open(gtfs_final_hdf5_path)  # read csv

    trip_list = df["trip_id"].unique().tolist()
    delayed_funcs = [delayed(get_stop_time_df)(t_id, get_conn) for t_id in trip_list]
    parallel_pool = Parallel(n_jobs=8)

    res = parallel_pool(delayed_funcs)

    stop_time_trip_df = vaex.from_pandas(pd.concat(res))
    print(f"concat stop_time data, time: {duration()}")

    stop_time_trip_df.export_hdf5(stop_time_data_path)  # export to hdf5
    return


def predict_traffic_from_scats(_df):
    print("*** scats predictions ***")

    df = _df.copy()
    df["hour"] = df["arrival_time"].apply(lambda t: get_dt(t, "%H:%M:%S").hour)
    df["dow"] = df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])

    pca_coord = vaex.ml.PCA(features=["lat", "lon"], n_components=2, prefix="pca")
    df = pca_coord.fit_transform(df)

    cycl_transform_hour = vaex.ml.CycleTransformer(features=["hour"], n=24)
    df = cycl_transform_hour.fit_transform(df)

    cycl_transform_dow = vaex.ml.CycleTransformer(features=["dow"], n=7)
    df = cycl_transform_dow.fit_transform(df)

    # load the scats ml model
    scats_model = load(scats_model_path)

    # get the predictions from scats data
    df = scats_model.transform(df)
    print(f"made predictions, time: {duration()}")

    return df[_df.get_column_names() + ["p_avg_vol"]]


def transform_data(df):
    df["is_delayed"] = df["arrival"].apply(is_delay)

    # transform the features into more machine learning friendly vars
    pca_coord = vaex.ml.PCA(features=["lat", "lon"], n_components=2, prefix="pca")
    df = pca_coord.fit_transform(df)

    cycl_transform_angle = vaex.ml.CycleTransformer(features=["direction_angle"], n=360)
    df = cycl_transform_angle.fit_transform(df)

    # transform timestamp
    df["t_dow"] = df["timestamp"].apply(lambda t: get_dt(t, "%Y-%m-%d %H:%M:%S").weekday())
    df["t_hour"] = df["timestamp"].apply(lambda t: get_dt(t, "%Y-%m-%d %H:%M:%S").hour)
    df["t_minute"] = df["timestamp"].apply(lambda t: get_dt(t, "%Y-%m-%d %H:%M:%S").minute)
    df["t_second"] = df["timestamp"].apply(lambda t: get_dt(t, "%Y-%m-%d %H:%M:%S").second)

    # transform arrival
    df["arr_dow"] = df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])
    df["arr_hour"] = df["arrival_time"].apply(lambda t: get_dt(t, "%H:%M:%S").hour)
    df["arr_minute"] = df["arrival_time"].apply(lambda t: get_dt(t, "%H:%M:%S").minute)
    df["arr_second"] = df["arrival_time"].apply(lambda t: get_dt(t, "%H:%M:%S").second)

    cycl_transform_dow = vaex.ml.CycleTransformer(features=["t_dow", "arr_dow"], n=7)
    df = cycl_transform_dow.fit_transform(df)

    cycl_transform_hour = vaex.ml.CycleTransformer(features=["t_hour", "arr_hour"], n=24)
    df = cycl_transform_hour.fit_transform(df)

    cycl_transform_minute = vaex.ml.CycleTransformer(
        features=["t_minute", "t_second", "arr_minute", "arr_second"], n=60
    )
    df = cycl_transform_minute.fit_transform(df)

    label_encoder = vaex.ml.LabelEncoder(features=["trip_id", "route_id"], prefix="label_encode_")
    df = label_encoder.fit_transform(df)

    standard_scaler = vaex.ml.StandardScaler(features=["arrival_mean", "p_mean_vol"])
    df = standard_scaler.fit_transform(df)

    minmax_scaler = vaex.ml.MinMaxScaler(features=["p_avg_vol", "shape_dist_traveled", "shape_dist_between"])
    df = minmax_scaler.fit_transform(df)

    print(f"dataWrangling done, ready to create model, time: {duration()}s")
    return df


def train_gtfsr(df):
    print("*** gtfsr model training ***")

    feats = (
        df.get_column_names(regex="pca")
        + df.get_column_names(regex=".*_x")
        + df.get_column_names(regex=".*_y")
        + df.get_column_names(regex="standard_scaled_*")
        + df.get_column_names(regex="label_encode_*")
        + df.get_column_names(regex="minmax_scaled_*")
        + ["stop_sequence", "direction", "is_delayed"]
    )

    target = "arrival"
    prediction_name = "p_arrival"

    lgbm_params = {
        "boosting_type": "gbdt",
        "learning_rate": 0.3,
        "n_estimators": 300,
        "max_depth": 50,
        "num_leaves": 50,
        "num_iterations": 300,
    }

    models = [
        # lightgmb Regressor
        Predictor(
            features=feats,
            target=target,
            prediction_name=prediction_name + "_lgbm",
            model=lightgbm.LGBMRegressor(**lgbm_params, n_jobs=-1),
        ),
        # XGBoost Regressor
        Predictor(
            features=feats,
            target=target,
            prediction_name=prediction_name + "_xgb",
            model=xgb.XGBRegressor(max_depth=50, min_child_weight=1, n_estimators=300, n_jobs=-1, learning_rate=0.3),
        ),
    ]

    # here we fit and train the model
    for i, model in enumerate(models):
        model.fit(df)
        print(f"\n\nmodel {i} trained, time taken: {duration()}s")

        df = model.transform(df)

    prediction_final = df.p_arrival_lgbm.astype("float") * 0.5 + df.p_arrival_xgb.astype("int") * 0.5
    df[prediction_name + "_final"] = prediction_final

    df.state_write(os.path.join(outdir, "gtfsr_model.json"))
    print("exported model")
    return


def extract():
    print("started extraction process")
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    # execute only if run as a script
    process_gtfsr_to_csv()
    combine_csv()

    # remove the generated zip file at the end
    os.remove(gtfs_csv_zip)


def process_data():
    cols = ["trip_id", "stop_sequence", "stop_id", "start_time"]

    if not os.path.exists(stop_time_data_path):
        create_stop_time_data()

    df = vaex.open(gtfs_final_hdf5_path)

    df = vaex_mjoin(df, vaex.open(stop_time_data_path), cols, cols, how="inner")
    print(f"merge stop_time & gtfsr data, time: {duration()}")

    df = predict_traffic_from_scats(df)

    df.export_hdf5(gtfs_processed_path)
    print(f"finished processing data, {duration()}")


def create_model():
    df = vaex.open(gtfs_processed_path)
    df = df.sample(frac=1)

    # remove outliers from dataset, all delays over 20 minutes
    outlier = 60 * 20
    df = df[
        (df["arrival"] >= -outlier)
        & (df["arrival"] <= outlier)
        & (df["departure"] >= -outlier)
        & (df["departure"] <= outlier)
    ]

    df["arr_dow"] = df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])
    df["arr_hour"] = df["arrival_time"].apply(lambda t: get_dt(t, "%H:%M:%S").hour)
    df["arrival"] = df["arrival"].apply(lambda t: 0 if t == 0 else t / 60)

    cols = ["trip_id", "stop_id", "arr_dow", "arr_hour"]

    # if the arrival historical means dataset is not created we create it
    if not os.path.exists(gtfsr_historical_means_path):
        print("*** creating gtfsr historical means dataset ***")
        # creates a dataset of historical average means using the stop_id, arrival_day_of_week and trip_id identifiers

        vaex.from_pandas(
            (
                df.to_pandas_df()
                .groupby(cols)
                .agg({"arrival": "mean", "p_avg_vol": "mean"})
                .rename(columns={"arrival": "arrival_mean", "p_avg_vol": "p_mean_vol"})
                .reset_index()
            )
        ).export_hdf5(gtfsr_historical_means_path)

    # join the arrival means to our dataset
    df = vaex_mjoin(df, vaex.open(gtfsr_historical_means_path), cols, cols, how="left")

    # create a shallow copy in order to reset the new state
    df = df.shallow_copy()

    # transform our data
    df = transform_data(df)

    # train our data
    train_gtfsr(df)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--extract", help="extracts a zip of gtfsr records to a csv", action="store_true")
    parser.add_argument("--process", help="processes the created csv with the correct data format", action="store_true")
    parser.add_argument("--model", help="creates a prediction model using processed gtfsr data", action="store_true")
    parser.add_argument("--clear", help="clear temp data", action="store_true")
    parser.add_argument("--all", help="end2end extract process and create model", action="store_true")

    args = parser.parse_args()

    if args.extract:
        extract()

    if args.process:
        process_data()

    if args.model:
        create_model()

    if args.all:
        extract()
        process_data()
        create_model()

    if args.clear:
        os.remove(stop_time_data_path)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()