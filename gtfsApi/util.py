from multigtfs.models import(Agency, Block, FareRule, Fare, FeedInfo, Feed, Frequency,
                             Route, ServiceDate, Service, ShapePoint, Shape, StopTime,
                             Stop, Transfer, Trip, Zone)


def deleteGtfsModel():
    models = [Agency, Block, FareRule, Fare, FeedInfo, Feed, Frequency, Route,
              ServiceDate, Service, ShapePoint, Shape, StopTime, Stop, Transfer, Trip, Zone]

    for model in models:
        model.objects.all().delete()
        print(model.__name__, ' deleted')
