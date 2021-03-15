from django.test import TestCase
from urllib.parse import urljoin


class GtfsTest(TestCase):
    base_route = "/api/gtfs/"

    routes = [
        "agency",
        "feed",
        "route",
        "servicedate",
        "service",
        "shapepoint",
        "shape",
        "stoptime",
        "stop",
        "transfer",
        "trip",
        "query",
        "query/routes",
        "query/route_trips",
        "query/trip_stops",
        "query/stop_departures",
        "query/stop_departures_prediction",
    ]

    routes_query = [
        "query/routes/?short_name=27",
        "query/route_trips/?route_id=0",
        "query/trip_stops/?trip_id=0",
        "query/stop_departures/?stop_id=316",
        "query/stop_departures_prediction/?stop_id=0",
    ]

    routes_fail_query = [
        "query/routes/?short_name",
        "query/routes/?short_name=qwerty",
        "query/route_trips/?route_id",
        "query/route_trips/?route_id=qwerty",
        "query/trip_stops/?trip_id",
        "query/trip_stops/?trip_id=qwerty",
        "query/stop_departures/?stop_id",
        "query/stop_departures/?stop_id=qwerty",
        "query/stop_departures_prediction/?stop_id",
        "query/stop_departures_prediction/?stop_id=qwerty",
    ]

    def test_base_route(self):
        response = self.client.get(self.base_route)
        self.assertEqual(response.status_code, 200)

    # assert all GTFS routes are functioning
    def test_routes(self):
        for r in self.routes + self.routes_query:
            response = self.client.get(self.base_route + r + "/")
            self.assertEqual(response.status_code, 200)

    def test_route_data(self):
        for r in self.routes_query:
            response = self.client.get(self.base_route + r)

            self.assertContains(response, '"count":')
            self.assertContains(response, '"results":')
            self.assertEqual(response.status_code, 200)

    def test_route_fail(self):
        for r in self.routes_fail_query:
            response = self.client.get(self.base_route + r)

            self.assertNotContains(response, '"count":')
            self.assertNotContains(response, '"results":')
            self.assertEqual(response.status_code, 200)
