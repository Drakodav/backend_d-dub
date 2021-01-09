import os
from celery import shared_task
from django.core.checks.messages import ERROR, Error
from .models import GtfsRApi
import requests


@shared_task
def gtfs_r_api():
    headers = {
        'Accept': 'application/json',
        'Cache-Control': 'no-cache',
        'x-api-key': os.getenv("X_API_KEY"),
    }

    try:
        response = requests.get(
            url='https://api.nationaltransport.ie/gtfsrtest/?format=json', headers=headers)

        data = response.json()

        if "statusCode" in data and data["statusCode"] == 429:
            raise Exception('{}'.format(data["message"]))
        else:
            record = GtfsRApi(data=data)
            record.save()
            return 'Success {}'.format(record.timestamp)
    except Exception as e:
        return e