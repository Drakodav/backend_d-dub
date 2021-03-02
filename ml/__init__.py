import os
import vaex
from numpy import array

output_path = os.path.join(os.path.dirname(__file__), "processing", "output")
gtfsr_historical_means_path = os.path.join(output_path, "gtfsr_historical_means.hdf5")
stop_time_data_path = os.path.join(output_path, "stop_time_data.hdf5")
gtfsr_model_path = os.path.join(output_path, "gtfsr_model.json")


try:
    gtfsr_historical_means_df = vaex.open(gtfsr_historical_means_path)
    gtfsr_stop_time_df = vaex.open(stop_time_data_path)

    model_df = vaex.from_dict(
        {
            "trip_id": array(["19389.1.60-155-d12-1.89.O"], dtype=object),
            "start_date": array([20210302]),
            "start_time": array(["19:20:00"], dtype=object),
            "stop_sequence": array([24]),
            "arrival": array([5.0]),
            "timestamp": array(["2021-03-02 19:51:26"], dtype=object),
            "stop_id": array(["8220DB000264"], dtype=object),
            "arrival_time": array(["19:38:38"], dtype=object),
            "shape_dist_traveled": array([7818.16]),
            "direction": array(["0"], dtype=object),
            "route_id": array(["60-155-d12-1"], dtype=object),
            "lat": array([53.3535353]),
            "lon": array([-6.26225863]),
            "direction_angle": array([139.31470635]),
            "shape_dist_between": array([518.6]),
            "arr_dow": array([1]),
            "arr_hour": array([19]),
            "arrival_mean": array([6.0]),
            "p_mean_vol": array([68.53864425]),
        }
    )
    model_df.state_load(os.path.join(output_path, "gtfsr_model.json"))
    model_state = model_df.state_get()
except:
    print("no ml model found")


def get_hm_df():
    return gtfsr_historical_means_df.shallow_copy()


def get_st_df():
    return gtfsr_stop_time_df.shallow_copy()


def get_model():
    return model_state
