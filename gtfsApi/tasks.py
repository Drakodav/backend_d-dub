from celery.app import shared_task
from django.db import connection


@shared_task(ignore_result=False, track_started=True)
def delete_model():
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            TRUNCATE 
                Agency, Block, Fare_Rules, Fare, Feed_Info, Feed, Frequency, Route,
                Service_Date, Service, Shape_Point, Shape, Stop_Time, Stop, Transfer, Trip, Zone;
            """
        )
        cursor.close
        return 'success'
    except Exception as e:
        return e


@shared_task(ignore_result=False, track_started=True)
def delete_agency(id):

    # get agency routes
    routes = [r.id for r in Route.objects.filter(agency=id).iterator()]

    trips = []
    services = []
    shapes = []
    for r in routes:
        trip, service, shape = [], [], []
        for t in Trip.objects.filter(route=r).iterator():
            trip.append(t.id)
            service.append(t.service_id)
            shape.append(t.shape)
            t.delete()
        trips.extend(trip)
        services.extend(service)
        shapes.extend(shapes)
    print('deleted trips')

    for t in trips:
        for stop in StopTime.objects.filter(trip=t).values('stop').distinct():
            Stop.objects.get(id=stop).delete()

        StopTime.objects.filter(trip=t).delete()
    print('deleted stop and stoptime')

    for s in shapes:
        # delete shapePoints
        ShapePoint.objects.filter(shape=s).delete()

        # delete shapes
        Shape.objects.get(id=s).delete()
    print('deleted shape and shapepoints')

    for s in services:
        # delete serviceDate
        ServiceDate.objects.filter(service=s).delete()

        # delete service
        Service.objects.get(id=s).delete()
    print('service shape and servicedate')

    # deleting agency should be last since in case theres an error there is still a reference to that agency
    Agency.objects.get(id=id).delete()
    print('deleted agency')
    return 'success'
