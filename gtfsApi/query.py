
def route_stops_query(id: int, direction: int = 0):
    return """
SELECT stop.*
FROM stop, (
	SELECT stop_id, route_id
	FROM
		stop_time,(
			SELECT id, route_id, direction
			FROM trip
			WHERE route_id = {} and direction = '{}'
			GROUP BY shape_id, id, route_id, direction
		) AS unique_trips
	WHERE stop_time.trip_id = unique_trips.id
	GROUP BY stop_id, route_id
	) AS unique_stops
WHERE stop.id = unique_stops.stop_id; 
""".format(id, direction).lstrip()
