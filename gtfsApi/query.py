
def route_stops_query(id: int, direction: int = 0):
    return """
select 
 	 stop.id, stop.stop_id, stop.name, stop.point, stop_time.stop_sequence
from stop
left join stop_time on stop_time.stop_id = stop.id
left join trip on trip.id = stop_time.trip_id
left join service on service.id = trip.service_id
, (
	select 
		id,
		CAST(
			CASE
				WHEN extract(dow from current_date)=0 THEN services.sunday
				WHEN extract(dow from current_date)=1 THEN services.monday
				WHEN extract(dow from current_date)=2 THEN services.tuesday
				WHEN extract(dow from current_date)=3 THEN services.wednesday
				WHEN extract(dow from current_date)=4 THEN services.thursday
				WHEN extract(dow from current_date)=5 THEN services.friday
				WHEN extract(dow from current_date)=6 THEN services.saturday
			END as bool
		) as dow
	from service as services
	group by id, dow
) as my_service, (
	select my_trips.id
	from stop_time, (
		select trip.id
		from trip
		where trip.route_id = {} and trip.direction = '{}'
		group by trip.id
	) as my_trips
	where to_timestamp(stop_time.departure_time)::time >= current_time(0)::time
		and stop_time.stop_sequence = 1
		and stop_time.trip_id = my_trips.id
	order by stop_time.departure_time
	limit 10
) as one_trip
where my_service.id = service.id 
	and my_service.dow = true
	and trip.service_id = my_service.id 
 	and trip.id = one_trip.id
group by stop.id, stop_time.stop_sequence
order by stop_time.stop_sequence
;
""".format(id, direction).lstrip()
