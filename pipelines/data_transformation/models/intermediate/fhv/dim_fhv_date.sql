{{ config(
    materialized = 'table',
    schema       = 'fhv_intermediate'
) }}

/*
  Dimensión de tiempo FHV.
  Se obtiene la unión de fechas únicas de pickup y dropoff para cubrir
  todas las claves de fecha que usará fact_fhv_trips.
*/

WITH fechas_pickup AS (

    SELECT DISTINCT DATE(tpep_pickup_datetime) AS trip_date
    FROM {{ source('staging', 'fhv') }}
    WHERE tpep_pickup_datetime IS NOT NULL

),

fechas_dropoff AS (

    SELECT DISTINCT DATE(tpep_dropoff_datetime) AS trip_date
    FROM {{ source('staging', 'fhv') }}
    WHERE tpep_dropoff_datetime IS NOT NULL

),

todas_las_fechas AS (

    SELECT trip_date FROM fechas_pickup
    UNION
    SELECT trip_date FROM fechas_dropoff

),

final AS (

    SELECT
        TO_CHAR(trip_date, 'YYYYMMDD')::INTEGER  AS date_key,
        trip_date,
        EXTRACT(YEAR    FROM trip_date)::INTEGER  AS anio,
        EXTRACT(MONTH   FROM trip_date)::INTEGER  AS mes,
        EXTRACT(DAY     FROM trip_date)::INTEGER  AS dia,
        TO_CHAR(trip_date, 'TMDay')               AS nombre_dia,
        EXTRACT(DOW     FROM trip_date)::INTEGER  AS dia_semana,   -- 0=domingo
        EXTRACT(ISODOW  FROM trip_date)::INTEGER  AS dia_semana_iso, -- 1=lunes
        EXTRACT(WEEK    FROM trip_date)::INTEGER  AS semana_anio,
        EXTRACT(QUARTER FROM trip_date)::INTEGER  AS trimestre,
        CASE
            WHEN EXTRACT(ISODOW FROM trip_date) IN (6, 7) THEN TRUE
            ELSE FALSE
        END                                       AS es_fin_de_semana
    FROM todas_las_fechas
    WHERE trip_date IS NOT NULL

)

SELECT * FROM final
ORDER BY date_key
