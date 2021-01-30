from celery.app import shared_task
from celery_progress.backend import ProgressRecorder
from multigtfs.models import(Agency, Block, FareRule, Fare, FeedInfo, Feed, Frequency,
                             Route, ServiceDate, Service, ShapePoint, Shape, StopTime,
                             Stop, Transfer, Trip, Zone)


@shared_task(bind=True, ignore_result=True, track_started=True)
def deleteGtfsModel(self):
    models = [Agency, Block, FareRule, Fare, FeedInfo, Feed, Frequency, Route,
              ServiceDate, Service, ShapePoint, Shape, StopTime, Stop, Transfer, Trip, Zone]

    progress_recorder = ProgressRecorder(self)
    for model in models:
        query = model.objects.all()
        length = query.count()
        for i, record in enumerate(query.iterator(chunk_size=500)):
            record.delete()
            progress_recorder.set_progress(i+1, length)
        print(model.__name__, ' deleted', length, 'records')
    return 'success'
