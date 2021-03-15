from django.test import TestCase


class GtfsRTest(TestCase):
    base_route = "/api/gtfsr/"

    def test_base_route(self):
        response = self.client.get(self.base_route)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, '"timestamp":')
        self.assertContains(response, '"data":')
