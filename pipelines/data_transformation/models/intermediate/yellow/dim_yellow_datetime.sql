{{
  config(
    materialized = 'table',
    schema       = 'yellow_intermediate',
    tags         = ['yellow', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Fecha y hora para Yellow Taxi
  Granularidad horaria — misma lógica que green y fhv.
  Yellow tiene datos desde 2009, así que esta dimensión
  puede ser muy grande históricamente.
*/

WITH pickup_hours AS (
    SELECT DISTINCT DATE_TRUNC('hour', tpep_pickup_datetime) AS datetime_hour
    FROM {{ source('raw', 'formato_4') }}
    WHERE tpep_pickup_datetime IS NOT NULL
),
dropoff_hours AS (
    SELECT DISTINCT DATE_TRUNC('hour', tpep_dropoff_datetime) AS datetime_hour
    FROM {{ source('raw', 'formato_4') }}
    WHERE tpep_dropoff_datetime IS NOT NULL
),
all_hours AS (
    SELECT datetime_hour FROM pickup_hours
    UNION
    SELECT datetime_hour FROM dropoff_hours
)

SELECT
    datetime_hour                                               AS datetime_id,
    datetime_hour::DATE                                         AS full_date,
    EXTRACT(year    FROM datetime_hour)::INTEGER                AS year,
    EXTRACT(month   FROM datetime_hour)::INTEGER                AS month,
    EXTRACT(day     FROM datetime_hour)::INTEGER                AS day,
    EXTRACT(hour    FROM datetime_hour)::INTEGER                AS hour,
    TO_CHAR(datetime_hour, 'Day')                               AS day_name,
    TO_CHAR(datetime_hour, 'Mon')                               AS month_abbr,
    TO_CHAR(datetime_hour, 'Month')                             AS month_name,
    EXTRACT(dow     FROM datetime_hour)::INTEGER                AS day_of_week,
    EXTRACT(week    FROM datetime_hour)::INTEGER                AS week_of_year,
    EXTRACT(quarter FROM datetime_hour)::INTEGER                AS quarter,
    CASE WHEN EXTRACT(dow FROM datetime_hour) IN (0, 6)
         THEN TRUE ELSE FALSE END                               AS is_weekend,
    CASE
        WHEN EXTRACT(month FROM datetime_hour) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(month FROM datetime_hour) IN (3,  4, 5) THEN 'Spring'
        WHEN EXTRACT(month FROM datetime_hour) IN (6,  7, 8) THEN 'Summer'
        ELSE 'Fall'
    END                                                         AS season,
    CASE
        WHEN EXTRACT(hour FROM datetime_hour) BETWEEN 6  AND 11 THEN 'Morning'
        WHEN EXTRACT(hour FROM datetime_hour) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(hour FROM datetime_hour) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END                                                         AS time_of_day,
    CASE
        WHEN EXTRACT(dow FROM datetime_hour) BETWEEN 1 AND 5
         AND (
                EXTRACT(hour FROM datetime_hour) BETWEEN 7 AND 9
             OR EXTRACT(hour FROM datetime_hour) BETWEEN 16 AND 19
         )
        THEN TRUE ELSE FALSE
    END                                                         AS is_rush_hour
FROM all_hours
ORDER BY datetime_hour
