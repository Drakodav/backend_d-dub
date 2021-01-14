import os
import zipfile
from celery.app import shared_task
import requests
from .models import GtfsRApi
from dynamoDub.settings import STATIC_ROOT


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


@shared_task(bind=True, name="download_realtime_data", ignore_result=False, track_started=True)
def download_realtime_data(self, year: int, month: int):
    try:
        if GtfsRApi.objects.filter(timestamp__year=year, timestamp__month=month).exists():
            records = GtfsRApi.objects\
                .filter(timestamp__year=year, timestamp__month=month)\
                .values_list('data', flat=True)

            length = records.count()
            with zipfile.ZipFile(os.path.join(STATIC_ROOT, 'GtfsRRecords.zip'), 'w') as zf:
                for i, record in enumerate(records.iterator(chunk_size=1000)):
                    zf.writestr("{}.txt".format(i), str(record))

            return 'success'
        else:
            return None
    except Exception as e:
        return e
