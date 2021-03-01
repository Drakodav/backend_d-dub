import os
import vaex
from ml.processing.util import vaex_mjoin, apply_dow, get_dt
from ml.processing.gtfsr_util import predict_traffic_from_scats
from . import gtfsr_stop_time_df, gtfsr_historical_means_df, output_path


def make_prediction(data):
    st_df = gtfsr_stop_time_df.copy()
    hm_df = gtfsr_historical_means_df.copy()

    if not "start_time" in data or not "start_date" in data:
        return ""

    formatted_data = {
        "trip_id": [data["trip_id"]],
        "stop_sequence": [data["stop_sequence"]],
        "stop_id": [data["stop_id"]],
        "start_time": [data["start_time"]],
        "start_date": [data["start_date"]],
        "timestamp": [data["timestamp"]],
        "arrival": [data["arrival"]],
    }

    live_df = vaex.from_dict(formatted_data)
    # print(data["trip_id"] in st_df.trip_id.unique().tolist())

    cols = ["trip_id", "stop_sequence", "stop_id", "start_time"]
    live_df = vaex_mjoin(live_df, st_df, cols, cols, how="inner")

    if not len(live_df) == 1:
        return ""

    # # print(len(live_df))

    # join the arrival means to our dataset
    live_df["arr_dow"] = live_df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])
    live_df["arr_hour"] = live_df["arrival_time"].apply(lambda t: get_dt(t, "%H:%M:%S").hour)
    live_df["arrival"] = live_df["arrival"].apply(lambda t: 0 if t == 0 else t / 60)

    cols = ["trip_id", "stop_id", "arr_dow", "arr_hour"]
    live_df = vaex_mjoin(live_df, hm_df, cols, cols, how="inner")

    if not len(live_df) == 1:
        return ""

    # live_df.export_hdf5(os.path.join(output_path, "deploy_gtfsr.hdf5"))
    # print(live_df)
    # live_df.state_load(os.path.join(output_path, "gtfsr_model.json"))

    # # print(live_df[["p_arrival_lgbm"]])

    # return live_df[["p_arrival_lgbm"]][0]
    return ""
