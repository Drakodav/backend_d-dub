import os
import zipfile
from celery.app import shared_task
import requests
from .models import GtfsRApi
from celery_progress.backend import ProgressRecorder
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
            url='https://gtfsr.transportforireland.ie/v1/?format=json', headers=headers)

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
            progress_recorder = ProgressRecorder(self)
            records = GtfsRApi.objects\
                .filter(timestamp__year=year, timestamp__month=month)\
                .values_list('data', flat=True)

            length = records.count()
            with zipfile.ZipFile(os.path.join(STATIC_ROOT, 'GtfsRRecords.zip'), 'w') as zf:
                for i, record in enumerate(records.iterator(chunk_size=500)):
                    zf.writestr("{}.json".format(i), str(record),
                                compress_type=zipfile.ZIP_DEFLATED)
                    progress_recorder.set_progress(i+1, length)

            return 'success'
        else:
            return None
    except Exception as e:
        return e
