{{ config(
    materialized = 'materialized_view',
    schema       = 'fhv_marts'
) }}

/*
  Operaciones por franja horaria FHV.
  Responde: ¿Cuáles son las horas pico de demanda? ¿Cómo varía la duración
  de los viajes según la hora del día? ¿Qué franja genera más actividad?
*/

WITH base AS (

    SELECT
        f.anio,
        f.mes,
        f.pickup_hour,
        CASE
            WHEN f.pickup_hour BETWEEN 7  AND 9  THEN 'Hora pico mañana'
            WHEN f.pickup_hour BETWEEN 17 AND 19 THEN 'Hora pico tarde'
            WHEN f.pickup_hour BETWEEN 0  AND 5  THEN 'Madrugada'
            WHEN f.pickup_hour BETWEEN 10 AND 16 THEN 'Horario diurno'
            ELSE 'Horario nocturno'
        END                                        AS franja_horaria,

        COUNT(*)                                   AS total_viajes,
        ROUND(AVG(f.trip_duration_minutes), 2)     AS avg_duracion_min,
        ROUND(MIN(f.trip_duration_minutes), 2)     AS min_duracion_min,
        ROUND(MAX(f.trip_duration_minutes), 2)     AS max_duracion_min,
        COUNT(DISTINCT f.base_key)                 AS bases_activas

    FROM {{ ref('fact_fhv_trips') }} f
    WHERE f.trip_duration_minutes > 0
    GROUP BY 1, 2, 3, 4

)

SELECT
    *,
    ROUND(
        100.0 * total_viajes / NULLIF(SUM(total_viajes) OVER (PARTITION BY anio, mes), 0),
        2
    )                                              AS pct_viajes_del_mes,
    ROUND(
        100.0 * total_viajes / NULLIF(SUM(total_viajes) OVER (PARTITION BY anio, mes, franja_horaria), 0),
        2
    )                                              AS pct_dentro_franja
FROM base
ORDER BY anio, mes, pickup_hour
