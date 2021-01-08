from django.urls import path
from django.urls.conf import include
from .api import RealtimeGTFS

urlpatterns = [
    path('', RealtimeGTFS.as_view()),
]
