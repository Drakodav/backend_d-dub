import os
from celery.app import shared_task
import requests
from .models import GtfsRApi


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


@shared_task(name="download_realtime_data", ignore_result=False, track_started=True)
def download_realtime_data(year: int, month: int):
    try:
        if GtfsRApi.objects.filter(timestamp__year=year, timestamp__month=month).exists():
            records = GtfsRApi.objects\
                .filter(timestamp__year=year, timestamp__month=month)\
                .values_list('data', flat=True)[::1]

            return str(records)
    except Exception as e:
        return e
