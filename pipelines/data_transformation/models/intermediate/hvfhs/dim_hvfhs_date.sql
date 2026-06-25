{{ config(
    materialized = 'table',
    schema       = 'hvfhs_intermediate'
) }}

/*
  Dimensión de tiempo HVFHS.
  Se consolidan fechas únicas de los cuatro timestamps del viaje:
  request, on_scene, pickup y dropoff.
*/

WITH todas_las_fechas AS (

    SELECT DISTINCT DATE(tpep_pickup_datetime)  AS trip_date FROM {{ source('staging', 'hvfhs') }} WHERE tpep_pickup_datetime  IS NOT NULL
    UNION
    SELECT DISTINCT DATE(tpep_dropoff_datetime) AS trip_date FROM {{ source('staging', 'hvfhs') }} WHERE tpep_dropoff_datetime IS NOT NULL
    UNION
    SELECT DISTINCT DATE(request_datetime)      AS trip_date FROM {{ source('staging', 'hvfhs') }} WHERE request_datetime      IS NOT NULL
    UNION
    SELECT DISTINCT DATE(on_scene_datetime)     AS trip_date FROM {{ source('staging', 'hvfhs') }} WHERE on_scene_datetime     IS NOT NULL

),

final AS (

    SELECT
        TO_CHAR(trip_date, 'YYYYMMDD')::INTEGER  AS date_key,
        trip_date,
        EXTRACT(YEAR    FROM trip_date)::INTEGER  AS anio,
        EXTRACT(MONTH   FROM trip_date)::INTEGER  AS mes,
        EXTRACT(DAY     FROM trip_date)::INTEGER  AS dia,
        TO_CHAR(trip_date, 'TMDay')               AS nombre_dia,
        EXTRACT(DOW     FROM trip_date)::INTEGER  AS dia_semana,
        EXTRACT(ISODOW  FROM trip_date)::INTEGER  AS dia_semana_iso,
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
