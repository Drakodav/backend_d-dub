from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import generics, serializers
from .models import GtfsRApi


class MySerializer(serializers.ModelSerializer):
    class Meta:
        model = GtfsRApi
        fields = ['timestamp', 'data']


class RealtimeGTFS(APIView):

    def get(self, request, format=None):
        record = GtfsRApi.objects.order_by('timestamp').last()
        serializer = MySerializer(record)
        return Response(serializer.data)
