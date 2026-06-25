{{
  config(
    materialized = 'table',
    schema       = 'green_intermediate',
    tags         = ['green', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Fecha y hora
  Se construye a partir de los timestamps únicos (truncados a la hora)
  de pickup y dropoff en los datos de green taxi.

  La granularidad es por hora para mantener el volumen manejable
  y permitir análisis temporales detallados.
*/

WITH pickup_hours AS (
    SELECT DISTINCT DATE_TRUNC('hour', tpep_pickup_datetime) AS datetime_hour
    FROM {{ source('raw', 'formato_3') }}
    WHERE tpep_pickup_datetime IS NOT NULL
),

dropoff_hours AS (
    SELECT DISTINCT DATE_TRUNC('hour', tpep_dropoff_datetime) AS datetime_hour
    FROM {{ source('raw', 'formato_3') }}
    WHERE tpep_dropoff_datetime IS NOT NULL
),

all_hours AS (
    SELECT datetime_hour FROM pickup_hours
    UNION
    SELECT datetime_hour FROM dropoff_hours
)

SELECT
    -- PK: el timestamp truncado a la hora sirve como ID natural
    datetime_hour                                             AS datetime_id,

    -- Atributos de fecha
    datetime_hour::DATE                                       AS full_date,
    EXTRACT(year  FROM datetime_hour)::INTEGER                AS year,
    EXTRACT(month FROM datetime_hour)::INTEGER                AS month,
    EXTRACT(day   FROM datetime_hour)::INTEGER                AS day,
    EXTRACT(hour  FROM datetime_hour)::INTEGER                AS hour,

    -- Nombre del día y mes (en inglés, estándar NYC TLC)
    TO_CHAR(datetime_hour, 'Day')                             AS day_name,
    TO_CHAR(datetime_hour, 'Mon')                             AS month_abbr,
    TO_CHAR(datetime_hour, 'Month')                           AS month_name,

    -- Día de semana (0=domingo ... 6=sábado en Postgres)
    EXTRACT(dow FROM datetime_hour)::INTEGER                  AS day_of_week,

    -- Número de semana del año
    EXTRACT(week FROM datetime_hour)::INTEGER                 AS week_of_year,

    -- Quarter
    EXTRACT(quarter FROM datetime_hour)::INTEGER              AS quarter,

    -- Flags
    CASE
        WHEN EXTRACT(dow FROM datetime_hour) IN (0, 6) THEN TRUE
        ELSE FALSE
    END                                                       AS is_weekend,

    CASE
        WHEN EXTRACT(month FROM datetime_hour) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(month FROM datetime_hour) IN (3,  4, 5) THEN 'Spring'
        WHEN EXTRACT(month FROM datetime_hour) IN (6,  7, 8) THEN 'Summer'
        ELSE 'Fall'
    END                                                       AS season,

    -- Franja horaria del día
    CASE
        WHEN EXTRACT(hour FROM datetime_hour) BETWEEN 6  AND 11 THEN 'Morning'
        WHEN EXTRACT(hour FROM datetime_hour) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(hour FROM datetime_hour) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END                                                       AS time_of_day,

    -- Rush hour (tipico NYC: 7-9am y 4-7pm en días de semana)
    CASE
        WHEN EXTRACT(dow FROM datetime_hour) BETWEEN 1 AND 5
         AND (
                EXTRACT(hour FROM datetime_hour) BETWEEN 7 AND 9
             OR EXTRACT(hour FROM datetime_hour) BETWEEN 16 AND 19
         )
        THEN TRUE
        ELSE FALSE
    END                                                       AS is_rush_hour

FROM all_hours
ORDER BY datetime_hour
