from django.urls import path
from django.urls.conf import include
from .views import getRouterUrls

urlpatterns = [
    path('', include(getRouterUrls())),
]
