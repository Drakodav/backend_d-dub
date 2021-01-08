from celery import shared_task
from django.core.checks.messages import ERROR
from .models import GtfsRApi
import requests


@shared_task
def gtfs_r_api():
    headers = {
        'Accept': 'application/json',
        'Cache-Control': 'no-cache',
        'x-api-key': 'd8bd6f4116d44a6592f541e1b3954eee',
    }

    try:
        response = requests.get(
            url='https://api.nationaltransport.ie/gtfsrtest/?format=json', headers=headers)

        data = response.json()

        record = GtfsRApi(data=data)
        record.save()

        return 'Success'
    except ERROR as e:
        return e
