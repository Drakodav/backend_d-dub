import vaex
import os
import pandas as pd

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
gtfsr_model_df_path = os.path.join(outdir, "gtfsr_model.hdf5")

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


def combine_csv():
    temp = os.path.join(outdir, "gtfsr")
    combined_csv = pd.concat([pd.read_csv(f) for f in [temp + "_jan.csv", temp + "_feb.csv"]])
    combined_csv = combined_csv.drop_duplicates(subset=entity_cols[:5])
    combined_csv.to_csv(gtfs_final_csv_path, index=False, header=True)

    vaex.from_csv(gtfs_final_csv_path, convert=True, copy_index=False, chunk_size=1000000)


live_df = vaex.open(os.path.join(outdir, "deploy_gtfsr.hdf5"))
# live_df = vaex.open(os.path.join(outdir, "gtfsr_model.hdf5"))

live_df.state_load(os.path.join(outdir, "gtfsr_model.json"))

print(live_df)
