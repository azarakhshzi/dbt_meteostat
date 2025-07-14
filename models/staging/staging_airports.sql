WITH airports_regions_join AS (
    SELECT * 
    FROM {{ source('flights_data', 'airports') }} AS a
    LEFT JOIN {{ source('flights_data', 'regions') }} AS r
      ON a.iso_country = r.code
)

SELECT *
FROM airports_regions_join