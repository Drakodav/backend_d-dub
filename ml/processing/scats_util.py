from sklearn.ensemble import RandomForestRegressor
from vaex.ml.sklearn import Predictor
from joblib import parallel_backend, dump
from datetime import datetime
import pandas as pd
import argparse
import vaex.ml
import vaex
import time
import os

dir = os.path.dirname(__file__)
outdir = os.path.join(dir, "output")

model_out = os.path.join(outdir, "scats_model.json")
scatsFilesPath = os.path.join(dir, "data")
finalScatsPath = os.path.join(outdir, "scats.csv")


# here we process the scats data from multiple csv file to one single file
def process_scats_data():
    """Gets the dataset for this assignment
    Returns: dataset - pandas dataframe
    """

    start = time.time()
    print("starting scats_processing")

    scatsFiles = os.listdir(scatsFilesPath)

    scats_data = []
    sites_data = None

    for sFile in scatsFiles:
        datasetPath = os.path.join(scatsFilesPath, sFile)
        if "scats_volume_2020" in sFile:
            data = pd.read_csv(datasetPath, sep=",")
            scats_data.append(data)

            print(sFile, "time: {}s".format(round(time.time() - start)))
        elif "sites.csv" == sFile:
            sites_data = pd.read_csv(datasetPath, sep=",", na_values="NULL")
            print(sFile, "time: {}s".format(round(time.time() - start)))

    # merge the scats datsets
    scats_dataset = pd.concat(scats_data, ignore_index=True)

    # sites changes
    sites_data["Site"] = sites_data["SiteID"].fillna(0).astype(int)
    del sites_data["SiteID"]

    # create our dataset
    dataset = pd.merge(scats_dataset, sites_data, how="left", on=["Site"])

    # scats data changes
    dataset["Site_Description"] = dataset["Site_Description_Lower"]
    dataset["Region"] = dataset["Region_y"]
    del dataset["Weighted_Avg"]
    del dataset["Weighted_Var"]
    del dataset["Weighted_Std_Dev"]
    del dataset["Site_Description_Cap"]
    del dataset["Site_Description_Lower"]
    del dataset["Region_x"]
    del dataset["Region_y"]
    del dataset["Detector"]

    dataset = dataset[(dataset["Lat"] != 0.0) | (dataset["Long"] != 0)]

    dataset = dataset.dropna()  # delete any null values, I dont care at this point

    # we have to aggregate the detectors, this will significantly reduce the size of our dataset
    # there are about 31 detectors per site, for every hour. this makes for lots of duplicate data
    cols = {}
    for col in list(dataset.columns):
        if col in ["Sum_Volume", "Avg_Volume", "Site", "End_Time"]:
            continue
        cols[col] = "first"

    # group the data to aggregate the sum_volume and avg_volume
    grouped_data = dataset.groupby(["End_Time", "Site"], as_index=False).agg(
        {"Sum_Volume": ["sum"], "Avg_Volume": ["sum"], **cols}
    )
    grouped_data.columns = dataset.columns
    dataset = grouped_data

    print("grouped data, time: {}s".format(round(time.time() - start)))

    # convert end time to more meaningful values
    dows = []
    days = []
    hours = []
    months = []

    def apply_time(t):
        dtime = datetime.strptime(t, "%Y-%m-%d %H:%M:%S.%f")
        dows.append(dtime.weekday())
        hours.append(dtime.hour)
        months.append(dtime.month)
        days.append(dtime.day)
        return t

    dataset["End_Time"].apply(apply_time)
    dataset["dow"] = dows
    dataset["hour"] = hours
    dataset["month"] = months
    dataset["day"] = days
    del dataset["End_Time"]

    dataset.columns = ["site", "sum_vol", "avg_vol", "lat", "lon", "site_desc", "region", "dow", "hour", "month", "day"]

    print("processed data, time: {}s".format(round(time.time() - start)))

    dataset.to_csv(finalScatsPath, index=False, header=True)
    print("finished, time: {}s".format(round(time.time() - start)))
    return


# we create the scats model which can be used to predict unkown avg_volumes of traffic for a
# lat: radians, lon: radians, dayOfWeek: int, hourOfDay: int
def create_scats_ml_model():

    start = time.time()
    print("starting scats ml modeling")

    # load existing csv into vaex dataframe
    if not os.path.exists(finalScatsPath + ".hdf5"):
        vaex.from_csv(finalScatsPath, convert=True, copy_index=False, chunk_size=1_000_000)

    df = vaex.open(finalScatsPath + ".hdf5", shuffle=True)
    df = df.sample(frac=1)

    # transform the features into more machine learning friendly vars
    pca_coord = vaex.ml.PCA(features=["lat", "lon"], n_components=2, prefix="pca")
    df = pca_coord.fit_transform(df)

    cycl_transform_hour = vaex.ml.CycleTransformer(features=["hour"], n=24)
    df = cycl_transform_hour.fit_transform(df)

    cycl_transform_dow = vaex.ml.CycleTransformer(features=["dow"], n=7)
    df = cycl_transform_dow.fit_transform(df)

    feats = df.get_column_names(regex="pca") + df.get_column_names(regex=".*_x") + df.get_column_names(regex=".*_y")
    target = "avg_vol"

    print("dataWrangling done, ready to create model, time: {}s".format(round(time.time() - start)))

    # create a randomForestRegression model
    model = RandomForestRegressor(random_state=42, n_estimators=7 * 24)
    vaex_model = Predictor(features=feats, target=target, model=model, prediction_name="p_avg_vol")

    # here we fit and train the model
    with parallel_backend("threading", n_jobs=8):
        vaex_model.fit(df=df)
        print("\n\nmodel created, time: {}s".format(round(time.time() - start)))

    with parallel_backend("threading", n_jobs=8):
        dump(value=vaex_model, filename=model_out, compress=3)
        print("model written to output, time: {}s".format(round(time.time() - start)))

    print("model trained, time: {}s".format(round(time.time() - start)))
    return


def process_argv():
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    # execute only if run as a script
    process_scats_data()

    if os.path.exists(finalScatsPath + ".hdf5"):
        os.remove(finalScatsPath + ".hdf5")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--process", help="processes the created csv with the correct data format", action="store_true")
    parser.add_argument("--model", help="creates a prediction model using processed gtfsr data", action="store_true")
    parser.add_argument("-all", help="end2end extract process and create model", action="store_true")

    args = parser.parse_args()

    if args.process:
        process_argv()
    if args.model:
        # execute only if run as a script
        create_scats_ml_model()
    if args.all:
        process_argv()
        create_scats_ml_model()
