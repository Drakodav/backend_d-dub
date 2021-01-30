from django.contrib.gis.db.models.fields import LineStringField, MultiLineStringField
from rest_framework_gis.filters import GeometryFilter
from django.db import models
from multigtfs.models import(Agency, Block, FareRule, Fare, FeedInfo, Feed, Frequency,
                             Route, ServiceDate, Service, ShapePoint, Shape, StopTime,
                             Stop, Transfer, Trip, Zone)
from multigtfs.models.fields import SecondsField
from multigtfs.models.base import models as gtfsModel
from rest_framework import routers, serializers, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
import django_filters
from django_filters.rest_framework import FilterSet
from django.db.models.base import Model
from .actions import route_stops


# generate a default template filter to use in the viewset
def getFilterClass(my_model: Model):
    class MyFilter(FilterSet):
        class Meta:
            if my_model.__name__ == 'StopTime' or my_model.__name__ == 'Frequency':
                # Trip is a foreign key that can screw things up when filtering
                exclude = ['trip']

            model = my_model
            fields = ('__all__')    # include all the fields from the Model
            filter_overrides = {    # any model types mathing these will have use these filter classes
                models.CharField: {
                    'filter_class': django_filters.CharFilter,
                    'extra': lambda f: {
                        'lookup_expr': 'icontains',
                    },
                },
                SecondsField: {
                    'filter_class': django_filters.TimeFilter,
                    'extra': lambda f: {
                        'lookup_expr': 'icontains',
                    },
                },
                gtfsModel.PointField: {
                    'filter_class': GeometryFilter,
                    'extra': lambda f: {
                        'lookup_expr': 'icontains',
                    },
                },
                MultiLineStringField: {
                    'filter_class': GeometryFilter,
                    'extra': lambda f: {
                        'lookup_expr': 'icontains',
                    },
                },
                LineStringField: {
                    'filter_class': GeometryFilter,
                    'extra': lambda f: {
                        'lookup_expr': 'icontains',
                    },
                }
            }
    return MyFilter


# generate a default template serialize
def getSerializer(my_model: Model):
    class MySerializer(serializers.ModelSerializer):
        class Meta:
            model = my_model
            fields = ('__all__')
    return MySerializer


# generate a default template viewSet
def getViewSet(my_model: Model):
    class MyViewSet(viewsets.ReadOnlyModelViewSet):

        # Manually change the title name e.g. API Root/ "Agency Root"
        def __init__(self, **kwargs) -> None:
            self.name = my_model.__name__ + str(' List')
            super().__init__(**kwargs)

        queryset = my_model.objects.all().order_by('id')
        serializer_class = getSerializer(my_model)
        filterset_class = getFilterClass(my_model)

        @action(methods=['get'], detail=False)
        def newest(self, request):
            newest = self.get_queryset().order_by('id').last()
            serializer = self.get_serializer_class()(newest)
            return Response(serializer.data)

        if my_model.__name__ == 'Route':
            @action(methods=['get'], detail=False, url_name='stops', url_path='stops')
            def get_stops(self, request):
                return route_stops(self, request)
    return MyViewSet


# List all the models that need to be registered for our API
bulkRoutes = [
    ('agency', Agency, 'Agency'),
    # ('block', Block, 'Block'),
    # ('farerule', FareRule, 'FareRule'),
    # ('fare', Fare, 'Fare'),
    # ('feedinfo', FeedInfo, 'FeedInfo'),
    ('feed', Feed, 'Feed'),
    # ('frequency', Frequency, 'Frequency'),
    ('route', Route, 'Route'),
    ('servicedate', ServiceDate, 'ServiceDate'),
    ('service', Service, 'Service'),
    ('shapepoint', ShapePoint, 'ShapePoint'),
    ('shape', Shape, 'Shape'),
    ('stoptime', StopTime, 'StopTime'),
    ('stop', Stop, 'Stop'),
    ('transfer', Transfer, 'Transfer'),
    ('trip', Trip, 'Trip'),
    # ('zone', Zone, 'Zone')
]


# returns the registered url routes for the API
def getRouterUrls():
    router = routers.DefaultRouter()
    for r in bulkRoutes:
        router.register(r'{}'.format(r[0]), getViewSet(
            r[1]), basename='{}'.format(r[2]))
    return router.urls
