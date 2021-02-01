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

    queries = [
        # delete stop_time first            !!!revise
        """
        delete from stop_time
        where exists (
            select stimes.* 
            from stop_time as stimes, (
                select trips.id
                from trip as trips, (
                    select agency_id, id
                    from  route
                    where agency_id = {}
                    group by agency_id, id
                ) as my_routes
                where my_routes.id = trips.route_id
            ) as my_trips
            where stimes.trip_id = my_trips.id
        );
        """.format(id),
        # delete trip           !!! revise
        """
        delete from trip
        where exists (
            select trips.* 
            from trip as trips, (
                select agency_id, id
                from  route
                where agency_id = 168
                group by agency_id, id
            ) as my_routes
            where my_routes.id = trips.route_id
        );
        """.format(id),
        # delete routes not finished yet
        """delete from route where agency_id = {};""".format(id),
        # delete agency
        """delete from agency where id = {};""".format(id),

        # delete shape_points       !revise
        """
        DELETE FROM shape_point
        WHERE Exists (
            select points.*
            from shape_point as points, (
                select shapes.id
                from shape as shapes, (
                    select trips.route_id, trips.shape_id
                    from trip as trips, (
                        select agency_id, id
                        from  route
                        where agency_id = {}
                        group by agency_id, id
                    ) as my_routes
                    where my_routes.id = trips.route_id
                    group by route_id, shape_id
                ) as my_trips
                where shapes.id = my_trips.shape_id
                group by shapes.id
            ) as my_shapes
            where points.shape_id = my_shapes.id
        );
        """.format(id),
    ]

    return 'success'
