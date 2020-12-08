import copy
from rest_framework import routers, serializers
from rest_framework import viewsets
from multigtfs.models import(Agency, Block, FareRule, Fare, FeedInfo, Feed, Frequency,
                             Route, ServiceDate, Service, ShapePoint, Shape, StopTime,
                             Stop, Transfer, Trip, Zone)


def getSerializer(Model):
    class TempSerializer(serializers.ModelSerializer):
        class Meta:
            model = Model
            fields = ('__all__')
    return TempSerializer


def getViewSet(Model):
    class TempViewSet(viewsets.ReadOnlyModelViewSet):
        queryset = Model.objects.all()
        serializer_class = getSerializer(Model)
    return TempViewSet


bulkRoutes = [
    (r'agency', Agency, 'Agency'),
    (r'block', Block, 'Block'),
    (r'farerule', FareRule, 'FareRule'),
    (r'fare', Fare, 'Fare'),
    (r'feedinfo', FeedInfo, 'FeedInfo'),
    (r'feed', Feed, 'Feed'),
    (r'frequency', Frequency, 'Frequency'),
    (r'route', Route, 'Route'),
    (r'servicedate', ServiceDate, 'ServiceDate'),
    (r'service', Service, 'Service'),
    (r'shapepoint', ShapePoint, 'ShapePoint'),
    (r'shape', Shape, 'Shape'),
    (r'stoptime', StopTime, 'StopTime'),
    (r'stop', Stop, 'Stop'),
    (r'transfer', Transfer, 'Transfer'),
    (r'trip', Trip, 'Trip'),
    (r'zone', Zone, 'Zone')
]


def getRouterUrls():
    router = routers.DefaultRouter()
    for r in bulkRoutes:
        router.register(r'{}'.format(r[0]), getViewSet(
            r[1]), basename='{}'.format(r[2]))

    return router.urls
