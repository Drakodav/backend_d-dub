import os
from joblib import load, parallel_backend
import vaex
from ml.processing.util import vaex_mjoin, apply_dow


output_path = os.path.join(os.path.dirname(__file__), "processing", "output")

gtfs_csv_zip = os.path.join(output_path, "gtfsr_csv.zip")
gtfs_final_csv_path = os.path.join(output_path, "gtfsr.csv")
gtfs_final_hdf5_path = os.path.join(output_path, "gtfsr.csv.hdf5")
gtfs_processed_path = os.path.join(output_path, "gtfsr_processed.hdf5")
scats_model_path = os.path.join(output_path, "scats_model.json")
gtfsr_processing_temp = os.path.join(output_path, "processing_temp.hdf5")
gtfsr_arrival_means = os.path.join(output_path, "gtfsr_arrival_means.hdf5")
stop_time_data_path = os.path.join(output_path, "stop_time_data.hdf5")


# scats_model = load(scats_model_path)
gtfsr_arrival_means_df = vaex.open(gtfsr_arrival_means)
gtfsr_stop_time_df = vaex.open(stop_time_data_path)

# vx_df = vaex.open(os.path.join(output_path, "gtfsr_processed.hdf5"))[:1]
# vx_df["arr_dow"] = vx_df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])
# # add arrival historical mean
# cols = ["trip_id", "stop_id", "arr_dow"]
# vx_df = vaex_mjoin(vx_df, gtfsr_arrival_means_df, cols, cols)

# gtfsr_model_state = vx_df.state_load(os.path.join(output_path, "gtfsr_model.json")).state_get()
