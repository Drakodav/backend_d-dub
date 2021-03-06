import os
import zipfile
from celery.app import shared_task
import requests
from .models import GtfsRApi
from celery_progress.backend import ProgressRecorder
from django.utils import timezone
from dynamoDub.settings import STATIC_ROOT
import json


@shared_task
def gtfs_r_api():
    headers = {
        "Accept": "application/json",
        "Cache-Control": "no-cache",
        "x-api-key": os.getenv("X_API_KEY"),
    }

    try:
        response = requests.get(url="https://gtfsr.transportforireland.ie/v1/?format=json", headers=headers)

        data = response.json()

        if "statusCode" in data and data["statusCode"] == 429:
            raise Exception("{}".format(data["message"]))
        else:
            if not len(GtfsRApi.objects.all().filter(id=1)) == 1:
                # Save the first entry
                # this line without any if statements will simply save the entries
                # removed ML from production
                GtfsRApi(data=data).save()
            else:
                record = GtfsRApi.objects.get(id=1)
                record.data = data
                record.timestamp = timezone.now()
                record.save()

            return "Success {}".format(record.timestamp)
    except Exception as e:
        return str(e)


@shared_task(bind=True, name="download_realtime_data", ignore_result=False, track_started=True)
def download_realtime_data(self, year: int, month: int):
    try:
        if GtfsRApi.objects.filter(timestamp__year=year, timestamp__month=month).exists():
            progress_recorder = ProgressRecorder(self)
            records = GtfsRApi.objects.filter(timestamp__year=year, timestamp__month=month).values_list(
                "data", flat=True
            )

            length = records.count()
            with zipfile.ZipFile(os.path.join(STATIC_ROOT, "GtfsRRecords.zip"), "w") as zf:
                for i, record in enumerate(records.iterator(chunk_size=500)):
                    if i % 10 == 0:
                        progress_recorder.set_progress(i, length)
                        zf.writestr("{}.json".format(i), json.dumps(record), compress_type=zipfile.ZIP_DEFLATED)

            return "success"
        else:
            return None
    except Exception as e:
        return str(e)
