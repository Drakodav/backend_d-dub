from __future__ import unicode_literals
from datetime import datetime
import logging

from django.db import connection
from django.conf import settings
from django.core.management.base import BaseCommand

from celery.app import shared_task
from .query import delete_agency_query, delete_model_query
from django.db import transaction
from multigtfs.models import Agency, Feed, Service


@shared_task(ignore_result=False, track_started=True)
def import_gtfs(gtfs_feed='gtfs.zip', name=None):
    # wget -O gtfs.zip https://www.transportforireland.ie/transitData/google_transit_combined.zip
    # place this file in the root of you project

    unset_name = 'Imported at %s' % datetime.now()
    name = name or unset_name

    # Setup logging
    verbosity = 1
    console = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    logger_name = 'multigtfs'
    if verbosity == 0:
        level = logging.WARNING
    elif verbosity == 1:
        level = logging.INFO
    elif verbosity == 2:
        level = logging.DEBUG
    else:
        level = logging.DEBUG
        logger_name = ''
        formatter = logging.Formatter(
            '%(name)s - %(levelname)s - %(message)s')
    console.setLevel(level)
    console.setFormatter(formatter)
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.addHandler(console)

    # Disable database query logging
    if settings.DEBUG:
        connection.use_debug_cursor = False

    feed = Feed.objects.create(name=name)
    feed.import_gtfs(gtfs_feed)

    # Set name based on feed
    if feed.name == unset_name:
        try:
            agency = feed.agency_set.order_by('id')[:1].get()
        except Agency.DoesNotExist:
            agency = None
        try:
            service = feed.service_set.order_by('id')[:1].get()
        except Service.DoesNotExist:
            service = None

        if agency:
            name = agency.name
            if service:
                name += service.start_date.strftime(' starting %Y-%m-%d')
            else:
                name += ' i' + unset_name[1:]
            feed.name = name
            feed.save()

    return ("Successfully imported Feed %s\n" % (feed))


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
def delete_agency(id: int, name: str):

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
    return 'success deleted {}'.format(name)
