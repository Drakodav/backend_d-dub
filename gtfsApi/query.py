
def route_stops_query(id: int, direction: int = 0):
    return """
SELECT stop.*, unique_stops.stop_sequence
FROM stop, (
	SELECT stop_id, route_id, stop_sequence
	FROM stop_time,(
		select 
			my_trips.*,
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
		from service as services, (
			select id, route_id, direction, service_id
			from trip as trips
			where trips.route_id = {} and direction = '{}'
			GROUP BY shape_id, id, route_id, direction, service_id
		) as my_trips
		where my_trips.service_id = services.id
	) AS unique_trips
	WHERE 
		stop_time.trip_id = unique_trips.id 
		and 
		to_timestamp(arrival_time)::time >= current_time(0)::time
		and 
		unique_trips.dow = true
	GROUP BY stop_id, route_id, stop_time.stop_sequence, stop_time.arrival_time
	order by stop_time.stop_sequence, to_timestamp(arrival_time)::time
	) AS unique_stops
WHERE stop.id = unique_stops.stop_id
group by stop.stop_id, stop.id, unique_stops.stop_sequence
;
""".format(id, direction).lstrip()


# """
# SELECT stop.*
# FROM stop, (
# 	SELECT stop_id, route_id
# 	FROM stop_time,(
# 		SELECT id, route_id, direction
# 		FROM trip
# 		WHERE route_id = {} and direction = '{}'
# 		GROUP BY shape_id, id, route_id, direction
# 	) AS unique_trips
# 	WHERE stop_time.trip_id = unique_trips.id
# 	GROUP BY stop_id, route_id
# 	) AS unique_stops
# WHERE stop.id = unique_stops.stop_id
# group by stop.stop_id, stop.id;
# """.format(id, direction).lstrip()
