from django.db import models
from django.contrib.postgres.fields import JSONField
from django.utils import timezone

# Create your models here.


class GtfsRApi(models.Model):
    data = models.JSONField()
    timestamp = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return 'gtfsRApi {}'.format(self.timestamp)
