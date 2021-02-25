from django.db import models
from django.contrib.postgres.indexes import HashIndex
from django.utils import timezone

# Create your models here.


class GtfsRApi(models.Model):
    data = models.JSONField()
    timestamp = models.DateTimeField(default=timezone.now, db_index=True)

    def __str__(self):
        return "gtfsRApi {}".format(self.timestamp)

    class Meta:
        indexes = [HashIndex(fields=["timestamp"])]
