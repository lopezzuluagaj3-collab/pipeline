{{ config(
    materialized = 'materialized_view',
    schema       = 'fhv_marts'
) }}

/*
  Operaciones diarias FHV.
  Responde: ¿Cuántos viajes hubo por día? ¿Cuál es la duración promedio?
  ¿Qué días de la semana tienen mayor demanda? ¿Cuántas bases estuvieron activas?
*/

WITH trips AS (

    SELECT
        f.pickup_date_key,
        f.trip_duration_minutes,
        f.base_key,
        f.affiliated_base_key,
        f.anio,
        f.mes
    FROM {{ ref('fact_fhv_trips') }} f
    WHERE f.trip_duration_minutes > 0

),

daily_agg AS (

    SELECT
        d.trip_date,
        d.anio,
        d.mes,
        d.dia,
        d.nombre_dia,
        d.dia_semana_iso,
        d.semana_anio,
        d.trimestre,
        d.es_fin_de_semana,

        COUNT(*)                                                           AS total_viajes,
        COUNT(DISTINCT t.base_key)                                         AS bases_despachantes_activas,
        COUNT(DISTINCT t.affiliated_base_key)                              AS bases_afiliadas_activas,

        ROUND(AVG(t.trip_duration_minutes), 2)                             AS avg_duracion_min,
        ROUND(MIN(t.trip_duration_minutes), 2)                             AS min_duracion_min,
        ROUND(MAX(t.trip_duration_minutes), 2)                             AS max_duracion_min,
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.trip_duration_minutes),
            2
        )                                                                  AS mediana_duracion_min,
        ROUND(
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY t.trip_duration_minutes),
            2
        )                                                                  AS p90_duracion_min

    FROM trips t
    JOIN {{ ref('dim_fhv_date') }} d ON t.pickup_date_key = d.date_key
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

)

SELECT
    *,
    ROUND(
        100.0 * total_viajes / NULLIF(SUM(total_viajes) OVER (PARTITION BY anio, mes), 0),
        2
    ) AS pct_viajes_del_mes
FROM daily_agg
ORDER BY trip_date
