from celery.app import shared_task
from django.db import connection
from .query import delete_agency_query, delete_model_query
from django.db import transaction
from multigtfs.models import Agency


@shared_task(ignore_result=False, track_started=True)
def delete_model():
    cursor = connection.cursor()
    try:
        with transaction.atomic():
            with transaction.atomic():
                cursor.execute(delete_model_query())

    except Exception as e:
        cursor.close
        raise e

    cursor.close
    return 'success'


@shared_task(ignore_result=False, track_started=True)
def delete_agency(id: int):

    cursor = connection.cursor()
    try:
        with transaction.atomic():
            if Agency.objects.filter(id=id).exists():
                with transaction.atomic():
                    cursor.execute(delete_agency_query(id))
            else:
                cursor.close
                return 'id: {}, does not exist in the database'.format(id)
    except Exception as e:
        cursor.close
        raise e

    cursor.close
    return 'success'
