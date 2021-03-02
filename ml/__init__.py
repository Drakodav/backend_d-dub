import os
from joblib import load, parallel_backend
import vaex

output_path = os.path.join(os.path.dirname(__file__), "processing", "output")
gtfsr_historical_means_path = os.path.join(output_path, "gtfsr_historical_means.hdf5")
stop_time_data_path = os.path.join(output_path, "stop_time_data.hdf5")


try:
    gtfsr_historical_means_df = vaex.open(gtfsr_historical_means_path)
    gtfsr_stop_time_df = vaex.open(stop_time_data_path)
except:
    print("no ml model found")


def get_hm_df():
    return gtfsr_historical_means_df.shallow_copy()


def get_st_df():
    return gtfsr_stop_time_df.shallow_copy()
