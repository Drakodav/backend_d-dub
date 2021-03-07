from datetime import datetime, timedelta
import numpy as np
import itertools
import psycopg2
import vaex
import time
import re


# parse a string to datetime
def get_dt(dt, format):
    return datetime.strptime(str(dt), format)


# lets return a iterable that can be chunked
def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


# calculate the direction angle from point 1 to point 2
# in our case we use first stop and last stop lon lats
def direction_angle(theta_1, phi_1, theta_2, phi_2):
    dtheta = theta_2 - theta_1
    dphi = phi_2 - phi_1
    radians = np.arctan2(dtheta, dphi)
    return np.rad2deg(radians)


# return the dow for arrival time and departure time.
# since a trip that starts at 11.30pm can  have an arrival time of 00:01am
# therefore its the next day
def apply_dow(start_date, start_time, expected_time):
    date = get_dt(start_date, "%Y%m%d")
    if get_dt(start_time, "%H:%M:%S") > get_dt(expected_time, "%H:%M:%S"):
        return (date + timedelta(days=1)).weekday()
    return date.weekday()


# connect to the PostgreSQL server
def get_conn():
    return psycopg2.connect(host="localhost", port="25432", database="gis", user="docker", password="docker")


# Template Function: connect to database and run query.
def run_query(query: str = ""):
    """ Connect to the PostgreSQL database server """
    try:
        conn = get_conn()

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        cur.execute(query)
        data = cur.fetchall()

        # close the communication with the PostgreSQL
        cur.close()
        return data
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# realtime trip_id can be quite unstable,
# this functions attempts to alleviate it by searching and replacing
# observed anomalies.
def find_trip_regex(trip_list, trip_id):
    if not type(trip_id) == str:
        return None

    tokens = trip_id.split(".")
    if not len(tokens) == 5:
        return None

    route_id = tokens[2].split("-")

    if route_id[2] in ["ga2", "gad"]:
        route_id[2] = "ga[2|d]"
        tokens[2] = "-".join(route_id)
    elif route_id[2] in ["d12", "b12"]:
        route_id[2] = "[b|d]12"
        tokens[2] = "-".join(route_id)

    if "y" in tokens[1]:
        tokens[1] = "*"

    tokens[3] = "*"

    reg = ".".join(tokens)

    r = re.compile(reg)
    matched_list = list(filter(r.match, trip_list))

    if len(matched_list) > 0:
        return matched_list[0]
    else:
        return None


# join multiple vaex columns
def vaex_mjoin(x_left, x_right, keys_left: list, keys_right: list, how: str):
    assert how in ["left", "right", "inner"], "how must be one of 'left', 'right' or 'inner'"

    assert (
        type(keys_left) == list and type(keys_right) == list
    ), f"keys must be list and list, not {type(keys_left)} and {type(keys_right)}"

    assert len(keys_left) == len(
        keys_right
    ), f"lenghts of left and right keys dont match, left: {len(keys_left)} right: {len(keys_right)}"

    if len(keys_left) == 1:
        join_result = x_left.join(
            x_right, left_on=keys_left[0], right_on=keys_right[0], how=how, allow_duplication=True
        )

    elif len(keys_left) > 1:
        for idx, zp in enumerate(zip(keys_left, keys_right)):
            left_key, right_key = zp

            assert (
                left_key in x_left.get_column_names()
            ), f"left key {left_key} doesnt exist in {x_left.get_column_names()}"
            assert (
                right_key in x_right.get_column_names()
            ), f"right key {right_key} doesnt exist in {x_right.get_column_names()}"

            add_left = x_left[left_key]
            add_right = x_right[right_key]

            if not add_left.dtype == str:
                add_left = add_left.astype(str)
            if not add_right.dtype == str:
                add_right = add_right.astype(str)

            if idx == 0:
                x_right["group_right"] = add_right
                x_left["group_left"] = add_left
            else:
                x_right["group_right"] = x_right["group_right"] + "_" + add_right
                x_left["group_left"] = x_left["group_left"] + "_" + add_left

        x_right = x_right.drop(keys_right)
        join_result = x_left.join(x_right, left_on="group_left", right_on="group_right", how=how)
        join_result = join_result.drop(["group_left", "group_right"])
    return join_result


# label encodes if an arrival update is either delayed, on time or early
def is_delay(arrival):
    if arrival > 0:
        return 1
    elif arrival == 0:
        return 0
    elif arrival < 0:
        return -1