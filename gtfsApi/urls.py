from django.urls import path
from django.urls.conf import include
from .api import getRouterUrls

urlpatterns = [
    path('', include(getRouterUrls())),
]
