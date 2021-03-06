import os

import vaex
from django.conf import settings
from django.test import TestCase
from numpy import array

output_path = os.path.join(settings.BASE_DIR, "ml", "processing", "output")
gtfsr_historical_means_path = os.path.join(output_path, "gtfsr_historical_means.hdf5")
stop_time_data_path = os.path.join(output_path, "stop_time_data.hdf5")
gtfsr_model_path = os.path.join(output_path, "gtfsr_model.json")


class MlTest(TestCase):
    model_df = vaex.from_dict(
        {
            "route_id": array(["60-155-d12-1"], dtype=object),
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

    def test_files(self):
        [
            self.assertEqual(os.path.exists(p), True)
            for p in [gtfsr_historical_means_path, stop_time_data_path, gtfsr_model_path]
        ]

    def test_model(self):
        self.model_df.state_load(gtfsr_model_path)

        pred_val = self.model_df[["p_arrival_lgbm"]][0][0]
        self.assertTrue(pred_val)
