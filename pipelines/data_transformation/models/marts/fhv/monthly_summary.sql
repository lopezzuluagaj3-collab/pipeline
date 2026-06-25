{{ config(
    materialized = 'materialized_view',
    schema       = 'fhv_marts'
) }}

/*
  Resumen mensual FHV — KPIs de alto nivel.
  Responde: ¿Cómo evolucionan los viajes mes a mes y año a año?
  ¿Cuál es la tendencia de duración promedio? ¿Cuántas bases estuvieron activas?
*/

WITH monthly AS (

    SELECT
        f.anio,
        f.mes,

        COUNT(*)                                           AS total_viajes,
        COUNT(DISTINCT f.base_key)                         AS bases_despachantes_activas,
        COUNT(DISTINCT f.affiliated_base_key)              AS bases_afiliadas_activas,

        ROUND(AVG(f.trip_duration_minutes), 2)             AS avg_duracion_min,
        ROUND(SUM(f.trip_duration_minutes) / 60.0, 2)     AS total_horas_viaje,
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.trip_duration_minutes),
            2
        )                                                  AS mediana_duracion_min,
        ROUND(
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.trip_duration_minutes),
            2
        )                                                  AS p90_duracion_min

    FROM {{ ref('fact_fhv_trips') }} f
    WHERE f.trip_duration_minutes > 0
    GROUP BY 1, 2

),

with_lag AS (

    SELECT
        *,
        LAG(total_viajes,    1)  OVER (ORDER BY anio, mes) AS viajes_mes_anterior,
        LAG(total_viajes,   12)  OVER (ORDER BY anio, mes) AS viajes_mismo_mes_anio_anterior,
        LAG(avg_duracion_min, 1) OVER (ORDER BY anio, mes) AS avg_duracion_mes_anterior,
        LAG(bases_despachantes_activas, 1) OVER (ORDER BY anio, mes) AS bases_activas_mes_anterior

    FROM monthly

)

SELECT
    *,

    CASE
        WHEN viajes_mes_anterior > 0
        THEN ROUND(100.0 * (total_viajes - viajes_mes_anterior) / viajes_mes_anterior, 2)
    END                                                    AS mom_trip_growth_pct,

    CASE
        WHEN viajes_mismo_mes_anio_anterior > 0
        THEN ROUND(100.0 * (total_viajes - viajes_mismo_mes_anio_anterior) / viajes_mismo_mes_anio_anterior, 2)
    END                                                    AS yoy_trip_growth_pct,

    CASE
        WHEN avg_duracion_mes_anterior > 0
        THEN ROUND(100.0 * (avg_duracion_min - avg_duracion_mes_anterior) / avg_duracion_mes_anterior, 2)
    END                                                    AS mom_avg_duration_change_pct,

    CASE
        WHEN bases_activas_mes_anterior > 0
        THEN ROUND(100.0 * (bases_despachantes_activas - bases_activas_mes_anterior) / bases_activas_mes_anterior, 2)
    END                                                    AS mom_bases_growth_pct

FROM with_lag
ORDER BY anio, mes
