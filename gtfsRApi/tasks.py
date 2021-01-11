import os
from celery import shared_task
from django.http.response import FileResponse
from .models import GtfsRApi
from dynamoDub.settings import BASE_DIR
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


# from gtfsRApi.tasks import download_realtime_data
# download_realtime_data(2021, 1)
@shared_task
def download_realtime_data(year: int, month: int):

    if GtfsRApi.objects.filter(timestamp__year=year, timestamp__month=month).exists():
        records = GtfsRApi.objects\
            .filter(timestamp__year=year, timestamp__month=month)\
            .values_list('data', flat=True)[::1]
        # f = open(os.path.join(BASE_DIR, 'gtfsRecords.txt'), 'w+')
        # f.write(str(records))
        # f.close()

        return FileResponse(str(records), as_attachment=True, filename='gtfsRecords.txt')
    else:
        print("choose another date")
