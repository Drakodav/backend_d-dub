def delete_agency_query(agency_id: int):
    return """
delete from stop_time
where stop_time.id in (
	select stimes.id
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
	group by stimes.id
);

delete from trip
where trip.id in (
    select trips.id 
    from trip as trips, (
        select agency_id, id
        from route
        where agency_id = {}
        group by agency_id, id
    ) as my_routes
    where my_routes.id = trips.route_id
	group by trips.id
);

delete from route
where route.id in (
	select route.id 
	from route 
	where not exists (
	   select
	   from trip
	   where trip.route_id = route.id
	)
	group by route.id
);

delete from agency where id = {};

delete from stop
where stop.id in (
	select stops.id
	from stop as stops 
	where not exists (
	   select
	   from stop_time
	   where stop_time.stop_id = stops.id
	)
	group by stops.id
);

delete from shape
where shape.id in (
	select shapes.id 
	from shape as shapes
	where not exists (
		select
		from trip
		where trip.shape_id = shapes.id
	)
	group by shapes.id
);

delete from shape_point
where shape_point.id in (
	select point.id 
	from shape_point as point
	where not exists (
		select
		from shape
		where shape.id = point.shape_id
	)
	group by point.id
);

delete from service
where service.id in (
	select services.id 
	from service as services
	where not exists (
		select
		from trip
		where trip.service_id = services.id
	)
	group by services.id
);

delete from service_date
where service_date.id in (
	select sdate.id 
	from service_date as sdate
	where not exists (
		select
		from service
		where service.id = sdate.service_id
	)
	group by sdate.id
);

delete from block
where block.id in (
	select blocks.id 
	from block as blocks
	where not exists (
		select
		from trip
		where trip.block_id = blocks.id
	)
	group by blocks.id
);

delete from transfer
where transfer.id in (
	select transfers.id
	from transfer as transfers 
	where not exists (
	   select
	   from stop
	   where stop.id = transfers.from_stop_id
	) or not exists (
	   select
	   from stop
	   where stop.id = transfers.to_stop_id
	) group by transfers.id
);
""".format(
        agency_id, agency_id, agency_id
    ).lstrip()


def delete_model_query():
    return """
TRUNCATE 
    Agency, Block, Fare_Rules, Fare, Feed_Info, Feed, Frequency, Route,
    Service_Date, Service, Shape_Point, Shape, Stop_Time, Stop, Transfer, Trip, Zone;
""".lstrip()
