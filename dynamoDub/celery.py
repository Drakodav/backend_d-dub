import os

from celery import Celery

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dynamoDub.settings')

app = Celery('dynamoDub')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

app.conf.beat_schedule = {
    # Executes every 80 seconds.
    "gtfs_r_api": {
        "task": "gtfsRApi.tasks.gtfs_r_api",
        "schedule": 62.0,  # run every X seconds
    },
    "download_realtime_data": {
        "task": "gtfsRApi.tasks.download_realtime_data",
    },
}
