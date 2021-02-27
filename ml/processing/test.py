import time
import vaex
import vaex.ml
import os

from util import apply_dow, vaex_mjoin


dir = os.path.dirname(__file__)
outdir = os.path.join(dir, "output")
gtfs_processed_path = os.path.join(outdir, "gtfsr_processed.hdf5")
gtfsr_arrival_means = os.path.join(outdir, "gtfsr_arrival_means.hdf5")

start = time.time()

df = vaex.open(gtfs_processed_path)[:20]


df["arr_dow"] = df.apply(apply_dow, ["start_date", "start_time", "arrival_time"])
# add arrival historical mean
cols = ["trip_id", "stop_id", "arr_dow"]
df = vaex_mjoin(df, vaex.open(gtfsr_arrival_means), cols, cols)

df.state_load(os.path.join(outdir, "gtfsr_model.json"))

print(df["arrival", "p_arrival_final"])
