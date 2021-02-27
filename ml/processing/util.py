from datetime import datetime, timedelta
import itertools
import psycopg2
import time
import re

# return the time taken until now
def duration(start):
    return round(time.time() - start)


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

    tokens[3] = "*"

    reg = ".".join(tokens)

    r = re.compile(reg)
    matched_list = list(filter(r.match, trip_list))

    if len(matched_list) > 0:
        return matched_list[0]
    else:
        return None