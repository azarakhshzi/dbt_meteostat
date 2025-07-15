with flights_cleaned AS ( 
	SELECT * 
	FROM {{ref('prep_flights')}}
),
unique_departures as (
	SELECT COUNT(DISTINCT origin) AS num_unique_departure --unique number of departures connections
	FROM flights_cleaned),
unique_arrivals AS ( 
	SELECT COUNT(DISTINCT dest) AS num_unique_arrival --unique number of arrival connections
	FROM flights_cleaned),
total_flight AS (
	SELECT num_unique_departure + num_unique_arrival AS total_flights -- ow many flight were planned in total (departures & arrivals)
	FROM unique_departures,unique_arrivals ),
canceled_flights AS ( 
	SELECT count(*)  AS total_cancelled_flights -- how many flights were canceled in total (departures & arrivals)
	FROM flights_cleaned 
	WHERE cancelled = 1 
	),
diverted_flights AS ( 
	SELECT count(*)  AS total_diverted_flights  --how many flights were diverted in total (departures & arrivals)
	FROM flights_cleaned 
	WHERE diverted = 1 
	),
occurred_flights AS ( 
	SELECT count(*)  AS total_occurred_flights -- how many flights actually occured in total (departures & arrivals)
	FROM flights_cleaned 
	WHERE cancelled = 0 AND diverted = 0 
	),
avg_unique_airplanes_travelled AS ( 
	SELECT ROUND(AVG(daily_unique_airplane), 2) AS avg_daily_unique_airplane_travelled -- how many unique airplanes travelled on average
	FROM (
		SELECT 
		flight_date, 
		count (DISTINCT tail_number) AS daily_unique_airplane
		FROM flights_cleaned 
		WHERE tail_number IS NOT NULL
		GROUP BY flight_date
		) subquery
	),
avg_unique_airplanes_serviced AS ( 
	SELECT ROUND(AVG(daily_unique_airplane), 2) AS avg_daily_unique_airplane_serviced -- how many unique airlines were in service on average
	FROM (
		SELECT 
		flight_date, 
		count (DISTINCT tail_number) AS daily_unique_airplane
		FROM flights_cleaned
		WHERE cancelled = 0
		GROUP BY flight_date
		) subquery
		),
	airport_cleaned AS (
		SELECT faa, name, city, country -- -- add city, country and name of the airport
		FROM {{ref('prep_airports')}}
    )
	SELECT 
		num_unique_departure,
		num_unique_arrival,
		total_flights,
		total_cancelled_flights,
		total_diverted_flights,
		total_occurred_flights,
		avg_daily_unique_airplane_travelled,
		avg_daily_unique_airplane_serviced,
	
	FROM unique_departures
	CROSS JOIN unique_arrivals
	CROSS JOIN total_flight
	CROSS JOIN canceled_flights
	CROSS JOIN diverted_flights
	CROSS JOIN occurred_flights
	CROSS JOIN avg_unique_airplanes_travelled
	CROSS JOIN avg_unique_airplanes_serviced