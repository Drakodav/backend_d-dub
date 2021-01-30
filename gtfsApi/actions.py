from django.db import connection
from rest_framework.response import Response
from django.contrib.gis.geos import GEOSGeometry
import json


def route_stops(self, request):
    message = [
        'id attribute needs to be included in order to retrieve the correct data',
        'e.g. {}/api/gtfs/route/stops/?id=<number>'.format(
            self.request.get_host())
    ]
    if 'id' in request.GET:
        try:
            id = int(request.GET['id'])
        except:
            message.append('id attribute must be integer value')
            return Response(message)

        cursor = connection.cursor()
        cursor.execute(
            "SELECT T3.* FROM trip AS T1 JOIN stop_time AS T2 ON T1.id=T2.trip_id AND route_id = {} JOIN stop AS T3 ON T2.stop_id=T3.id GROUP BY T3.id, T3.name;".format(id))

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

        # data = [
        #     dict(zip([col[0] for col in desc], row))
        #     for row in cursor.fetchall()
        # ]

        return Response(data)
    return Response(message)
