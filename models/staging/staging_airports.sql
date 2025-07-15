WITH airports_regions_join AS (
    SELECT * 
    FROM flights, airports
    LEFT JOIN flights
	LEFT JOIN regions
    USING (country)
)
SELECT * FROM airports_regions_join