{{ config(
    materialized = 'materialized_view',
    schema       = 'fhv_marts'
) }}

/*
  Rendimiento mensual por base despachante FHV.
  Responde: ¿Qué bases despachan más viajes? ¿Cuál es su tendencia de
  crecimiento mes a mes y año a año? ¿Con cuántas bases afiliadas operan?
*/

WITH base_monthly AS (

    SELECT
        f.anio,
        f.mes,
        b.dispatching_base_num,

        COUNT(*)                                   AS total_viajes,
        ROUND(AVG(f.trip_duration_minutes), 2)     AS avg_duracion_min,
        ROUND(SUM(f.trip_duration_minutes), 2)     AS total_minutos_viaje,
        COUNT(DISTINCT f.affiliated_base_key)      AS bases_afiliadas_distintas,

        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.trip_duration_minutes),
            2
        )                                          AS mediana_duracion_min

    FROM {{ ref('fact_fhv_trips') }} f
    JOIN {{ ref('dim_fhv_base') }} b ON f.base_key = b.base_key
    WHERE f.trip_duration_minutes > 0
    GROUP BY 1, 2, 3

),

with_lag AS (

    SELECT
        *,
        LAG(total_viajes, 1)  OVER (PARTITION BY dispatching_base_num ORDER BY anio, mes)
            AS viajes_mes_anterior,
        LAG(total_viajes, 12) OVER (PARTITION BY dispatching_base_num ORDER BY anio, mes)
            AS viajes_mismo_mes_anio_anterior,
        LAG(avg_duracion_min, 1) OVER (PARTITION BY dispatching_base_num ORDER BY anio, mes)
            AS avg_duracion_mes_anterior
    FROM base_monthly

)

SELECT
    *,
    CASE
        WHEN viajes_mes_anterior > 0
        THEN ROUND(100.0 * (total_viajes - viajes_mes_anterior) / viajes_mes_anterior, 2)
    END                                            AS mom_trip_growth_pct,

    CASE
        WHEN viajes_mismo_mes_anio_anterior > 0
        THEN ROUND(100.0 * (total_viajes - viajes_mismo_mes_anio_anterior) / viajes_mismo_mes_anio_anterior, 2)
    END                                            AS yoy_trip_growth_pct,

    CASE
        WHEN avg_duracion_mes_anterior > 0
        THEN ROUND(100.0 * (avg_duracion_min - avg_duracion_mes_anterior) / avg_duracion_mes_anterior, 2)
    END                                            AS mom_avg_duration_change_pct,

    -- Ranking mensual por volumen de viajes
    RANK() OVER (
        PARTITION BY anio, mes
        ORDER BY total_viajes DESC
    )                                              AS rank_viajes_mes

FROM with_lag
ORDER BY anio, mes, total_viajes DESC
