import vaex
from ml.processing.util import vaex_mjoin, apply_dow, get_dt
from ml.apps import MlConfig

# converts input data into the correct format for predictions to happen.
# !IMPORTANT LESSONS LEARNT! -> everything must be the same, in a vaex pipeline/state,
# both datatypes must be the same for every feature, also, an expression wont be recognised when loading
# the state on.
def make_prediction(data):
    st_df = MlConfig.st_df
    hm_df = MlConfig.hm_df
    model = MlConfig.state_model

    empty = ("", "")

    if not "start_time" in data or not "start_date" in data:
        return empty

    formatted_data = {
        "trip_id": [data["trip_id"]],
        "stop_sequence": [data["stop_sequence"]],
        "stop_id": [data["stop_id"]],
        "start_time": [data["start_time"]],
        "start_date": [int(data["start_date"])],
        "timestamp": [str(data["timestamp"])],
        "arrival": [float(data["arrival"] / 60)],
    }

    live_df = vaex.from_dict(formatted_data)

    # join stop time data, filtering improves speed by only copying relevant rows
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
        return empty

    # join the historical means to our dataset
    live_df["arr_dow"] = live_df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])
    live_df["arr_hour"] = live_df["arrival_time"].apply(lambda t: get_dt(t, "%H:%M:%S").hour)

    temp_df = hm_df[
        (hm_df["trip_id"] == data["trip_id"])
        & (hm_df["stop_id"] == data["stop_id"])
        & (hm_df["arr_dow"] == live_df[["arr_dow"]][0][0])
        & (hm_df["arr_hour"] == live_df[["arr_hour"]][0][0])
    ]

    if not len(temp_df) > 0:
        return empty

    cols = ["trip_id", "stop_id", "arr_dow", "arr_hour"]
    live_df = vaex_mjoin(
        live_df,
        temp_df.copy(),
        cols,
        cols,
        how="inner",
    )

    if not len(live_df) == 1:
        return empty

    # assert same type
    live_df["direction"] = live_df["direction"].astype("int64")

    # materialize virtual columns to match model state
    live_df = live_df.materialize("arr_dow")
    live_df = live_df.materialize("arr_hour")

    try:
        live_df.state_set(model)

        if len(live_df) == 1:
            return (round(live_df[["p_arrival_lgbm"]][0][0]) * 60), live_df[["p_arrival_lgbm"]][0][0]
    except:
        return empty
    return empty
