from django.db import connection
from rest_framework.response import Response
from django.contrib.gis.geos import GEOSGeometry
from .query import route_stops_query
import json


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
        cursor.execute(route_stops_query(route_id, direction))

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
