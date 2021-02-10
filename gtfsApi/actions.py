from django.db import connection
from rest_framework.response import Response
from django.contrib.gis.geos import GEOSGeometry
from .query import route_stops_query, stop_departures_query, trip_from_route_query
from gtfsRApi.models import GtfsRApi
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import ParseDict, MessageToDict
from django.conf import settings
import requests
import json


def get_stops_action(self, request):
    urlPath = self.request.get_host() + self.request.get_full_path()
    message = [
        'trip_id attribute needs to be included in order to retrieve the correct data',
        'e.g. {}?trip_id=<number>'.format(urlPath),
    ]

    if 'trip_id' in request.GET:
        try:
            trip_id = int(request.GET['trip_id'])
        except:
            message.append(
                'trip_id and direction attributes must be integer value')
            return Response(message)

        cursor = connection.cursor()
        cursor.execute(route_stops_query(trip_id))
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

        trip_ids = [r["trip_id"] for r in parsed_data]

        if settings.PRODUCTION:
            realtime_data = GtfsRApi.objects.order_by('id').last().data
        else:
            realtime_data = requests.get(
                url='https://api.thev-lad.com/api/gtfsr/').json()["data"]

        feed = gtfs_realtime_pb2.FeedMessage()
        ParseDict(realtime_data, feed)

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                stop_time_update = entity.trip_update.stop_time_update
                trip_id = str(entity.trip_update.trip.trip_id).replace(
                    '-b12-', '-d12-', 1)

                if trip_id in trip_ids:
                    idx = trip_ids.index(trip_id)
                    curr_stop_sequence = parsed_data[idx]["stop_sequence"]

                    departure = 0
                    arrival = 0
                    for stop_update in stop_time_update:
                        if stop_update.stop_sequence <= curr_stop_sequence:
                            if stop_update.HasField('departure'):
                                departure = 0 if stop_update.departure.delay == 0 \
                                    else departure + stop_update.departure.delay
                            if stop_update.HasField('arrival'):
                                arrival = 0 if stop_update.arrival.delay == 0 \
                                    else arrival+stop_update.arrival.delay

                    parsed_data[idx]["time_delta"] = {
                        'arrival': arrival, 'departure': departure}
                    # parsed_data[idx]["trip_update"] = MessageToDict(
                    #     trip_update)

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
