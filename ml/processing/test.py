import time
import vaex
import vaex.ml
import os
import pandas as pd

from ml.processing.util import apply_dow, vaex_mjoin, get_dt


dir = os.path.dirname(__file__)
outdir = os.path.join(dir, "output")
gtfs_records_zip = os.path.join(dir, "data", "GtfsRRecords.zip")
gtfs_csv_zip = os.path.join(outdir, "gtfsr_csv.zip")
gtfs_final_csv_path = os.path.join(outdir, "gtfsr.csv")
gtfsr_processed_path = os.path.join(outdir, "gtfsr_processed.hdf5")
scats_model_path = os.path.join(outdir, "scats_model.json")
gtfsr_processing_temp = os.path.join(outdir, "processing_temp.hdf5")
gtfsr_arrival_means = os.path.join(outdir, "gtfsr_arrival_means.hdf5")
gtfs_final_hdf5_path = os.path.join(outdir, "gtfsr.csv.hdf5")


finalScatsPath = os.path.join(outdir, "scats.csv")

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


temp = os.path.join(outdir, "gtfsr")
combined_csv = pd.concat([pd.read_csv(f) for f in [temp + "_jan.csv", temp + "_feb.csv"]])

combined_csv = combined_csv.drop_duplicates(subset=entity_cols[:5])

combined_csv.to_csv(gtfs_final_csv_path, index=False, header=True)

vaex.from_csv(gtfs_final_csv_path, convert=True, copy_index=False, chunk_size=1000000)

# start = time.time()

# df = vaex.open(gtfsr_processed_path)[:20]


# df["arr_dow"] = df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])
# # add arrival historical mean
# cols = ["trip_id", "stop_id", "arr_dow"]
# df = vaex_mjoin(df, vaex.open(gtfsr_arrival_means), cols, cols)

# df.state_load(os.path.join(outdir, "gtfsr_model.json"))

# print(df["arrival", "p_arrival_final"])


# print("*** scats predictions ***")

# df = vaex.open(gtfsr_processed_path)[:1]

# df.drop(["p_avg_vol"], inplace=True)

# print(df.get_column_names())


# df["hour"] = df["arrival_time"].apply(lambda t: get_dt(t, "%H:%M:%S").hour)
# df["dow"] = df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])

# # pca_coord = vaex.ml.PCA(features=["lat", "lon"], n_components=2, prefix="pca")
# # df = pca_coord.fit_transform(df)

# # cycl_transform_hour = vaex.ml.CycleTransformer(features=["hour"], n=24)
# # df = cycl_transform_hour.fit_transform(df)

# # cycl_transform_dow = vaex.ml.CycleTransformer(features=["dow"], n=7)
# # df = cycl_transform_dow.fit_transform(df)

# df.state_load(scats_model_path)

# print(df["p_avg_vol", "hour", "dow"])

# # return df[df.get_column_names(virtual=False) + ["p_avg_vol"]]
