from celery.app import shared_task
from multigtfs.models import (
    Agency, Block, FareRule, Fare, FeedInfo, Feed, Frequency,
    Route, ServiceDate, Service, ShapePoint, Shape, StopTime,
    Stop, Transfer, Trip, Zone
)


@shared_task(ignore_result=False, track_started=True)
def delete_model():
    models = [Agency, Block, FareRule, Fare, FeedInfo, Feed, Frequency, Route,
              ServiceDate, Service, ShapePoint, Shape, StopTime, Stop, Transfer, Trip, Zone]

    for model in models:
        query = model.objects.all()
        for record in query.iterator():
            record.delete()
        print(model.__name__ + ' deleted ' + ' records')
    return 'success'


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
