import os
from joblib import load, parallel_backend
import vaex
from ml.processing.util import vaex_mjoin, apply_dow


output_path = os.path.join(os.path.dirname(__file__), "processing", "output")
gtfsr_arrival_means = os.path.join(output_path, "gtfsr_historical_means.hdf5")
stop_time_data_path = os.path.join(output_path, "stop_time_data.hdf5")


try:
    gtfsr_arrival_means_df = vaex.open(gtfsr_arrival_means)
    gtfsr_stop_time_df = vaex.open(stop_time_data_path)
except:
    print("no ml model found")

# vx_df = vaex.open(os.path.join(output_path, "gtfsr_processed.hdf5"))[:1]
# vx_df["arr_dow"] = vx_df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])
# # add arrival historical mean
# cols = ["trip_id", "stop_id", "arr_dow"]
# vx_df = vaex_mjoin(vx_df, gtfsr_arrival_means_df, cols, cols)

# gtfsr_model_state = vx_df.state_load(os.path.join(output_path, "gtfsr_model.json")).state_get()
