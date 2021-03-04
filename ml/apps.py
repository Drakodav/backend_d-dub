from django.apps import AppConfig
from django.conf import settings
import vaex
import os
from numpy import array

output_path = os.path.join(settings.BASE_DIR, "ml", "processing", "output")
gtfsr_historical_means_path = os.path.join(output_path, "gtfsr_historical_means.hdf5")
stop_time_data_path = os.path.join(output_path, "stop_time_data.hdf5")
gtfsr_model_path = os.path.join(output_path, "gtfsr_model.json")


class MlConfig(AppConfig):
    name = "ml"

    if not all(
        [os.path.exists(p) == True for p in [gtfsr_historical_means_path, stop_time_data_path, gtfsr_model_path]]
    ):
        raise "not all ml models found"

    hm_df = vaex.open(gtfsr_historical_means_path)
    st_df = vaex.open(stop_time_data_path)

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
    model_df.state_load(gtfsr_model_path)
    state_model = model_df.state_get()
