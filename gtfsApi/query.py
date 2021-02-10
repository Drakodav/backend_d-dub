
def route_stops_query(trip_id: int):
    return """
select 
 	stop.id, stop.stop_id, stop.name, stop.point, stop_time.stop_sequence
from stop
left join stop_time on stop_time.stop_id = stop.id
left join trip on trip.id = stop_time.trip_id
where trip.id = {}
group by stop.id, stop_time.stop_sequence
order by stop_time.stop_sequence
;
""".format(trip_id).lstrip()


def stop_departures_query(stop_id: int):
    return """
select  
	to_timestamp(stop_time.departure_time)::time as departure_time,
	route.short_name,
	route.route_id,
	stop_time.stop_sequence,
 	trip.id, 
 	trip.trip_id, 
 	trip.headsign, 
 	trip.direction, 
 	trip.geometry
from trip
left join stop_time on trip.id = stop_time.trip_id
left join service on trip.service_id = service.id
left join service_date on service.id = service_date.service_id
left join route on trip.route_id = route.id
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
) as my_service
where stop_time.stop_id = {}
	and to_timestamp(stop_time.departure_time)::time >= current_timestamp(0)::time - interval '20min'
 	and CURRENT_DATE + to_timestamp(stop_time.departure_time)::time <= current_timestamp(0) + interval '1h 20min'
	and my_service.dow = true
	and trip.service_id = my_service.id 
	and not service.id in (select service_id from service_date where date = current_date )
group by trip.id, stop_time.id, route.id
order by stop_time.departure_time
;
""".format(stop_id).lstrip()


def trip_from_route_query(route_id: int, direction: int = 0):
    return """
select trip.* 
from trip
left join stop_time on stop_time.trip_id = trip.id
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
) as my_service
where my_service.dow = true
	and to_timestamp(stop_time.departure_time)::time >= current_time(0)::time
	and trip.service_id = my_service.id 
	and not my_service.id in (select service_id from service_date where date = current_date group by service_date.id)
	and trip.route_id = {} and trip.direction = '{}' 
group by trip.id,stop_time.id
order by stop_time.departure_time
limit 1
;
""".format(route_id, direction).lstrip()
