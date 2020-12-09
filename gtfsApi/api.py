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

def getFilterClass(Model):
    class MyFilter(FilterSet):
        class Meta:
            model = Model
            if Model.__name__ == 'StopTime' or Model.__name__ == 'Frequency':
                exclude = ['trip']

            fields = ('__all__')
            filter_overrides = {
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


def getSerializer(Model):
    class MySerializer(serializers.ModelSerializer):
        class Meta:
            model = Model
            fields = ('__all__')
    return MySerializer


def getViewSet(Model):
    class MyViewSet(viewsets.ReadOnlyModelViewSet):

        # Manually change the title name e.g. API Root/ "Agency Root"
        def __init__(self, **kwargs) -> None:
            self.name = Model.__name__ + str(' List')
            super().__init__(**kwargs)

        queryset = Model.objects.all().order_by('id')
        serializer_class = getSerializer(Model)
        filterset_class = getFilterClass(Model)

        @action(methods=['get'], detail=False)
        def newest(self, request):
            newest = self.get_queryset().order_by('id').last()
            serializer = self.get_serializer_class()(newest)
            return Response(serializer.data)

    return MyViewSet


bulkRoutes = [
    ('agency', Agency, 'Agency'),
    ('block', Block, 'Block'),
    ('farerule', FareRule, 'FareRule'),
    ('fare', Fare, 'Fare'),
    ('feedinfo', FeedInfo, 'FeedInfo'),
    ('feed', Feed, 'Feed'),
    ('frequency', Frequency, 'Frequency'),
    ('route', Route, 'Route'),
    ('servicedate', ServiceDate, 'ServiceDate'),
    ('service', Service, 'Service'),
    ('shapepoint', ShapePoint, 'ShapePoint'),
    ('shape', Shape, 'Shape'),
    ('stoptime', StopTime, 'StopTime'),
    ('stop', Stop, 'Stop'),
    ('transfer', Transfer, 'Transfer'),
    ('trip', Trip, 'Trip'),
    ('zone', Zone, 'Zone')
]


def getRouterUrls():
    router = routers.DefaultRouter()
    for r in bulkRoutes:
        router.register(r'{}'.format(r[0]), getViewSet(
            r[1]), basename='{}'.format(r[2]))

    return router.urls






