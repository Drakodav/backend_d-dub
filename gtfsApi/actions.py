from django.db import connection
from rest_framework.response import Response
from django.contrib.gis.geos import GEOSGeometry
import json


def query_route_stops(id: int, direction: int = 0):
    return """SELECT stop.*
                FROM
                stop,
                (SELECT stop_id, route_id
                FROM
                    stop_time,
                    (SELECT id, route_id, direction
                    FROM trip
                    WHERE route_id = {} and direction = '{}'
                    GROUP BY shape_id, id, route_id, direction
                    ) AS unique_trips
                WHERE stop_time.trip_id = unique_trips.id
                GROUP BY stop_id, route_id
                ) AS unique_stops
                WHERE stop.id = unique_stops.stop_id; """.format(id, direction).lstrip()


def route_stops(self, request):
    message = [
        'route_id attribute needs to be included in order to retrieve the correct data',
        'direction attribute is an optional requirement for the direction of the stops, default value=0, options are 0 & 1',
        'e.g. {}/api/gtfs/route/stops/?route_id=<number>'.format(
            self.request.get_host()),
        'e.g. {}/api/gtfs/route/stops/?route_id=<number>&direction=<number>'.format(
            self.request.get_host()),
    ]
    if 'route_id' in request.GET:
        direction = 0
        try:
            if 'direction' in request.GET:
                direction = int(request.GET['direction'])
            route_id = int(request.GET['route_id'])
        except:
            message.append(
                'route_id and direction attributes must be integer value')
            return Response(message)

        cursor = connection.cursor()
        cursor.execute(query_route_stops(route_id, direction))

        desc = cursor.description

        cols = [col[0] for col in desc]
        data = []
        for row in cursor.fetchall():

            obj = {}
            for x, y in zip(cols, row):
                if x == 'point':
                    point = {}
                    js = json.loads((GEOSGeometry(y).json))
                    for k in js:
                        point[k] = js[k]
                    obj[x] = point

                else:
                    obj[x] = y

            data.append(obj)

        cursor.close
        return Response(data)
    return Response(message)
