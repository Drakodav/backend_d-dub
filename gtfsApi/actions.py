from django.db import connection
from rest_framework.response import Response
from django.contrib.gis.geos import GEOSGeometry, geometry
from .query import route_stops_query, stop_departures_query, trip_from_route_query
import json


def get_stops_action(self, request):
    urlPath = self.request.get_host() + self.request.get_full_path()
    message = [
        'route_id attribute needs to be included in order to retrieve the correct data',
        'direction attribute is an optional requirement for the direction of the stops, default value=0, options are 0 & 1',
        'e.g. {}?route_id=<number>'.format(urlPath),
        'e.g. {}?route_id=<number>&direction=<number>'.format(urlPath),
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
        cursorData = cursor.fetchall()

        parsed_data = parse_data(cursorData, desc)

        cursor.close
        return Response(parsed_data)
    return Response(message)


def get_trips_action(self, request):
    urlPath = self.request.get_host() + self.request.get_full_path()
    message = [
        'route_id attribute needs to be included in order to retrieve the correct data',
        'direction attribute is an optional requirement for the direction of the stops, default value=0, options are 0 & 1',
        'e.g. {}?route_id=<number>'.format(urlPath),
        'e.g. {}?route_id=<number>&direction=<number>'.format(urlPath),
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
        cursor.execute(trip_from_route_query(route_id, direction))

        desc = cursor.description
        cursorData = cursor.fetchall()

        parsed_data = parse_data(cursorData, desc)

        cursor.close
        return Response(parsed_data)
    return Response(message)


def get_departures_action(self, request):
    urlPath = self.request.get_host() + self.request.get_full_path()
    message = [
        'stop_id attribute needs to be included in order to retrieve the correct data',
        'e.g. {}?stop_id=<number>'.format(urlPath),
    ]
    if 'stop_id' in request.GET:
        try:
            stop_id = int(request.GET['stop_id'])
        except:
            message.append(
                'stop_id and direction attributes must be integer value')
            return Response(message)

        cursor = connection.cursor()

        cursor.execute(stop_departures_query(stop_id))

        desc = cursor.description
        cursorData = cursor.fetchall()

        parsed_data = parse_data(cursorData, desc)
        cursor.close
        return Response(parsed_data)
    return Response(message)


def parse_data(data, desc):
    cols = [col[0] for col in desc]
    parsed_data = []
    for row in data:

        obj = {}
        for x, y in zip(cols, row):
            if x in ['point', 'geometry']:
                geom = {}
                js = json.loads((GEOSGeometry(y).json))
                for k in js:
                    geom[k] = js[k]
                obj[x] = geom

            else:
                obj[x] = y

        parsed_data.append(obj)
    return parsed_data
