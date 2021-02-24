from django.contrib.gis.db.models.fields import LineStringField, MultiLineStringField
from rest_framework_gis.filters import GeometryFilter
from django.db import models
from multigtfs.models import (
    Agency,
    Block,
    FareRule,
    Fare,
    FeedInfo,
    Feed,
    Frequency,
    Route,
    ServiceDate,
    Service,
    ShapePoint,
    Shape,
    StopTime,
    Stop,
    Transfer,
    Trip,
    Zone,
)
from multigtfs.models.fields import SecondsField
from multigtfs.models.base import models as gtfsModel
from rest_framework import routers, serializers, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
import django_filters
from django_filters.rest_framework import FilterSet
from django.db.models.base import Model
from .actions import get_departures_action, get_route_action, get_trips_action, get_stops_action


# generate a default template serialize
class QuerySerializer(serializers.Serializer):
    action = serializers.CharField()
    url = serializers.URLField()
    usage = serializers.CharField()


# generate a default template viewSet
class QueryViewSet(viewsets.ReadOnlyModelViewSet):

    # queryset = my_model.objects.all().order_by('id')
    serializer_class = QuerySerializer

    def get_queryset(self):
        urlPath = self.request.get_host() + self.request.get_full_path()
        messages = [
            ("", "", "Common queries are provided through the extra actions section"),
            ("get_routes", "route", "get routes based on todays timetable"),
            ("get_trips", "route_trip", "get a single trip from a route"),
            ("get_stops", "trip_stops", "get the stops assosciated with a trip"),
            ("get_departures", "stop_departures", "get trips assosciated to a stop"),
        ]
        array = [{"action": m[0], "url": "{}{}".format(urlPath, m[1]), "usage": m[2]} for m in messages]
        return list(array)

    @action(methods=["get"], detail=False, url_name="routes", url_path="routes")
    def get_routes(self, request):
        return get_route_action(self, request)

    @action(methods=["get"], detail=False, url_name="route_trips", url_path="route_trips")
    def get_trips(self, request):
        return get_trips_action(self, request)

    @action(methods=["get"], detail=False, url_name="trip_stops", url_path="trip_stops")
    def get_stops(self, request):
        return get_stops_action(self, request)

    @action(methods=["get"], detail=False, url_name="stop_departures", url_path="stop_departures")
    def get_departures(self, request):
        return get_departures_action(self, request)


# generate a default template filter to use in the viewset
def getFilterClass(my_model: Model):
    class MyFilter(FilterSet):
        class Meta:
            if my_model.__name__ == "StopTime" or my_model.__name__ == "Frequency":
                # Trip is a foreign key that can screw things up when filtering
                exclude = ["trip"]

            model = my_model
            fields = "__all__"  # include all the fields from the Model
            filter_overrides = {  # any model types mathing these will have use these filter classes
                models.CharField: {
                    "filter_class": django_filters.CharFilter,
                    "extra": lambda f: {
                        "lookup_expr": "icontains",
                    },
                },
                SecondsField: {
                    "filter_class": django_filters.TimeFilter,
                    "extra": lambda f: {
                        "lookup_expr": "icontains",
                    },
                },
                gtfsModel.PointField: {
                    "filter_class": GeometryFilter,
                    "extra": lambda f: {
                        "lookup_expr": "icontains",
                    },
                },
                MultiLineStringField: {
                    "filter_class": GeometryFilter,
                    "extra": lambda f: {
                        "lookup_expr": "icontains",
                    },
                },
                LineStringField: {
                    "filter_class": GeometryFilter,
                    "extra": lambda f: {
                        "lookup_expr": "icontains",
                    },
                },
            }

    return MyFilter


# generate a default template serialize
def getSerializer(my_model: Model):
    class MySerializer(serializers.ModelSerializer):
        class Meta:
            model = my_model
            fields = "__all__"

    return MySerializer


# generate a default template viewSet
def getViewSet(my_model: Model):
    class MyViewSet(viewsets.ReadOnlyModelViewSet):

        # Manually change the title name e.g. API Root/ "Agency Root"
        def __init__(self, **kwargs) -> None:
            self.name = my_model.__name__ + str(" List")
            super().__init__(**kwargs)

        queryset = my_model.objects.all().order_by("id")
        serializer_class = getSerializer(my_model)
        filterset_class = getFilterClass(my_model)

        @action(methods=["get"], detail=False)
        def newest(self, request):
            newest = self.get_queryset().order_by("id").last()
            serializer = self.get_serializer_class()(newest)
            return Response(serializer.data)

    return MyViewSet


# List all the models that need to be registered for our API
bulkRoutes = [
    ("agency", Agency, "Agency"),
    # ('block', Block, 'Block'),
    # ('farerule', FareRule, 'FareRule'),
    # ('fare', Fare, 'Fare'),
    # ('feedinfo', FeedInfo, 'FeedInfo'),
    ("feed", Feed, "Feed"),
    # ('frequency', Frequency, 'Frequency'),
    ("route", Route, "Route"),
    ("servicedate", ServiceDate, "ServiceDate"),
    ("service", Service, "Service"),
    ("shapepoint", ShapePoint, "ShapePoint"),
    ("shape", Shape, "Shape"),
    ("stoptime", StopTime, "StopTime"),
    ("stop", Stop, "Stop"),
    ("transfer", Transfer, "Transfer"),
    ("trip", Trip, "Trip"),
    # ('zone', Zone, 'Zone')
]


# returns the registered url routes for the API
def getRouterUrls():
    router = routers.DefaultRouter()
    for r in bulkRoutes:
        router.register(r"{}".format(r[0]), getViewSet(r[1]), basename="{}".format(r[2]))
    router.register("query", QueryViewSet, "Query")
    return router.urls
