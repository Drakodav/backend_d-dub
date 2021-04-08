import vaex
from ml.processing.util import vaex_mjoin, apply_dow, get_dt
from ml.apps import MlConfig

# converts input data into the correct format for predictions to happen.
# !IMPORTANT LESSONS LEARNT! -> everything must be the same, in a vaex pipeline/state,
# both datatypes must be the same for every feature, also, an expression wont be recognised when loading
# the state on.
def make_prediction(data):
    st_df = MlConfig.st_df  # stop_time_data
    hm_df = MlConfig.hm_df  # historical means dataset
    model = MlConfig.state_model  # GTFSR vaex model state

    empty = ("", "")

    if not "start_time" in data or not "start_date" in data:
        return empty

    formatted_data = {
        "route_id": [str(data["route_id"])],
        "direction": [int(data["direction"])],
        "stop_sequence": [int(data["stop_sequence"])],
        "stop_id": [str(data["stop_id"])],
        "start_time": [str(data["start_time"])],
        "start_date": [int(data["start_date"])],
        "timestamp": [str(data["timestamp"])],
        "arrival": [int(data["arrival"] / 60)],
    }

    live_df = vaex.from_dict(formatted_data)

    live_df["arr_dow"] = live_df.start_date.apply(lambda d: get_dt(d, "%Y%m%d").weekday())
    live_df.materialize("arr_dow", inplace=True)

    # print(live_df.dtypes, "\n", st_df.dtypes, "\n", hm_df.dtypes, "\n")

    temp_df = st_df[
        (st_df["route_id"] == live_df[["route_id"]][0][0])
        & (st_df["stop_sequence"] == live_df[["stop_sequence"]][0][0])
        & (st_df["stop_id"] == live_df[["stop_id"]][0][0])
        & (st_df["start_time"] == live_df[["start_time"]][0][0])
        & (st_df["direction"] == live_df[["direction"]][0][0])
    ].copy()

    if len(temp_df) < 1:
        return empty

    # join stop time data, filtering improves speed by only copying relevant rows
    cols = ["route_id", "stop_sequence", "stop_id", "start_time", "direction"]
    live_df = vaex_mjoin(live_df, temp_df, cols, cols, how="inner", allow_duplication=True)

    live_df["keep_trip"] = live_df.apply(
        lambda sd, dow: sd.replace("[", "").replace("]", "").replace(" ", "").split(",")[dow],
        ["service_days", "arr_dow"],
    )
    live_df = live_df[live_df.keep_trip == "True"]
    live_df.drop(["service_days", "keep_trip"], inplace=True)

    if len(live_df) < 1:
        return empty

    live_df["arr_hour"] = live_df["arrival_time"].apply(lambda t: get_dt(t, "%H:%M:%S").hour)
    live_df.materialize("arr_hour", inplace=True)

    # join the historical means to our dataset
    temp_df = hm_df[
        (hm_df["route_id"] == data["route_id"])
        & (hm_df["stop_id"] == data["stop_id"])
        & (hm_df["arr_dow"] == live_df[["arr_dow"]][0][0])
        & (hm_df["arr_hour"] == live_df[["arr_hour"]][0][0])
        & (hm_df["direction"] == int(data["direction"]))
        & (hm_df["stop_sequence"] == live_df[["stop_sequence"]][0][0])
    ].copy()

    if len(temp_df) < 1:
        return empty

    cols = ["route_id", "stop_id", "arr_dow", "arr_hour", "direction", "stop_sequence"]
    live_df = vaex_mjoin(
        live_df,
        temp_df,
        cols,
        cols,
        how="inner",
    )

    if len(live_df) < 1:
        return empty

    # assert same type
    live_df["direction"] = live_df["direction"].astype("int64")
    live_df["shape_dist_traveled"] = live_df["shape_dist_traveled"].astype("float64")
    live_df["lat"] = live_df["lat"].astype("float64")
    live_df["lon"] = live_df["lon"].astype("float64")
    live_df["direction_angle"] = live_df["direction_angle"].astype("float64")
    live_df["shape_dist_between"] = live_df["shape_dist_between"].astype("float64")

    # materialize virtual columns to match model state
    [
        live_df.materialize(col, inplace=True)
        for col in live_df.get_column_names()
        if not col in live_df.get_column_names(virtual=False)
    ]
    try:
        live_df.state_set(model)

        if len(live_df) > 0:
            return (round(live_df[["p_arrival_lgbm"]][0][0]) * 60), live_df[["p_arrival_lgbm"]][0][0]
    except:
        return empty
    return empty
