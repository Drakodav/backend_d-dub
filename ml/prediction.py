from datetime import datetime
import os
import vaex
from ml.processing.util import vaex_mjoin, apply_dow, get_dt
from ml.processing.gtfsr_util import predict_traffic_from_scats
from . import get_st_df, get_hm_df, output_path


def make_prediction(data):
    st_df = get_st_df()
    hm_df = get_hm_df()

    if not "start_time" in data or not "start_date" in data:
        return ""

    formatted_data = {
        "trip_id": [data["trip_id"]],
        "stop_sequence": [data["stop_sequence"]],
        "stop_id": [data["stop_id"]],
        "start_time": [data["start_time"]],
        "start_date": [int(data["start_date"])],
        "timestamp": [data["timestamp"]],
        "arrival": [data["arrival"]],
    }

    live_df = vaex.from_dict(formatted_data)
    # print(data["trip_id"] in st_df.trip_id.unique().tolist())

    cols = ["trip_id", "stop_sequence", "stop_id", "start_time"]
    live_df = vaex_mjoin(
        live_df,
        st_df[
            (st_df["trip_id"] == data["trip_id"])
            & (st_df["stop_sequence"] == data["stop_sequence"])
            & (st_df["stop_id"] == data["stop_id"])
            & (st_df["start_time"] == data["start_time"])
        ].copy(),
        cols,
        cols,
        how="inner",
    )

    if not len(live_df) == 1:
        return ""

    # # print(len(live_df))

    # join the arrival means to our dataset
    live_df["arr_dow"] = live_df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])
    live_df["arr_hour"] = live_df["arrival_time"].apply(lambda t: get_dt(t, "%H:%M:%S").hour)
    live_df["arrival"] = live_df["arrival"].apply(lambda t: 0 if t == 0 else t / 60)

    temp_df = hm_df[
        (hm_df["trip_id"] == data["trip_id"])
        & (hm_df["stop_id"] == data["stop_id"])
        & (hm_df["arr_dow"] == live_df[["arr_dow"]][0][0])
        & (hm_df["arr_hour"] == live_df[["arr_hour"]][0][0])
    ]

    if not len(temp_df) > 0:
        return ""

    cols = ["trip_id", "stop_id", "arr_dow", "arr_hour"]
    live_df = vaex_mjoin(
        live_df,
        temp_df.copy(),
        cols,
        cols,
        how="inner",
    )

    if not len(live_df) == 1:
        return ""

    live_df = live_df[
        [
            "trip_id",
            "start_date",
            "start_time",
            "stop_sequence",
            "arrival",
            "timestamp",
            "stop_id",
            "arrival_time",
            "shape_dist_traveled",
            "direction",
            "route_id",
            "lat",
            "lon",
            "direction_angle",
            "shape_dist_between",
            "arr_dow",
            "arr_hour",
            "arrival_mean",
            "p_mean_vol",
        ]
    ]

    live_df["direction"] = live_df["direction"].astype("int")

    # live_df.export_hdf5(os.path.join(output_path, "deploy_gtfsr.hdf5"))

    # live_df.state_load(os.path.join(output_path, "gtfsr_model.json"))
    print(live_df)

    # # print(live_df[["p_arrival_lgbm"]])

    # return live_df[["p_arrival_lgbm"]][0]
    return ""
