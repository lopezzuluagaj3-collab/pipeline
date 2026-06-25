{{ config(
    materialized = 'materialized_view',
    schema       = 'fhvhv_marts'
) }}

/*
  Rendimiento mensual por base y operador HVFHS.
  Responde: ¿Qué bases generan más ingresos? ¿Cuál es su tasa de crecimiento?
  ¿Qué porcentaje de sus viajes tienen discrepancia en el cobro?
*/

WITH base_monthly AS (

    SELECT
        f.anio,
        f.mes,
        b.dispatching_base_num,
        l.nombre_operador,

        COUNT(*)                                                        AS total_viajes,
        ROUND(AVG(f.trip_duration_minutes), 2)                          AS avg_duracion_min,
        ROUND(AVG(f.wait_time_minutes),     2)                          AS avg_espera_min,
        ROUND(AVG(f.trip_distance),         2)                          AS avg_distancia_millas,

        -- Revenue
        ROUND(SUM(f.fare_amount),                       2)              AS total_tarifa_base,
        ROUND(SUM(f.tip_amt),                           2)              AS total_propinas,
        ROUND(SUM(f.total_amt),                         2)              AS total_cobrado,
        ROUND(SUM(f.true_total_amt),                    2)              AS total_ideal,
        ROUND(SUM(f.true_total_amt) - SUM(f.total_amt), 2)              AS diferencia_cobro,
        ROUND(AVG(f.fare_amount),                       2)              AS avg_tarifa_viaje,
        ROUND(AVG(f.tip_amt),                           2)              AS avg_propina,

        -- Calidad de cobro
        COUNT(*) FILTER (WHERE NOT f.comparation_total_amt)             AS viajes_con_discrepancia,

        -- Servicio
        COUNT(*) FILTER (WHERE f.shared_match_flag)                     AS viajes_compartidos_exitosos,
        COUNT(*) FILTER (WHERE f.wav_match_flag)                        AS viajes_wav_completados

    FROM {{ ref('fact_hvfhs_trips') }} f
    JOIN {{ ref('dim_hvfhs_base') }}    b ON f.base_key    = b.base_key
    JOIN {{ ref('dim_hvfhs_license') }} l ON f.license_key = l.license_key
    GROUP BY 1, 2, 3, 4

),

with_lag AS (

    SELECT
        *,
        LAG(total_viajes,    1)  OVER (PARTITION BY dispatching_base_num ORDER BY anio, mes)
            AS viajes_mes_anterior,
        LAG(total_viajes,   12)  OVER (PARTITION BY dispatching_base_num ORDER BY anio, mes)
            AS viajes_mismo_mes_anio_anterior,
        LAG(total_cobrado,   1)  OVER (PARTITION BY dispatching_base_num ORDER BY anio, mes)
            AS cobrado_mes_anterior,
        LAG(total_cobrado,  12)  OVER (PARTITION BY dispatching_base_num ORDER BY anio, mes)
            AS cobrado_mismo_mes_anio_anterior
    FROM base_monthly

)

SELECT
    *,
    -- Crecimiento de viajes
    CASE
        WHEN viajes_mes_anterior > 0
        THEN ROUND(100.0 * (total_viajes - viajes_mes_anterior) / viajes_mes_anterior, 2)
    END                                                                 AS mom_trip_growth_pct,

    CASE
        WHEN viajes_mismo_mes_anio_anterior > 0
        THEN ROUND(100.0 * (total_viajes - viajes_mismo_mes_anio_anterior) / viajes_mismo_mes_anio_anterior, 2)
    END                                                                 AS yoy_trip_growth_pct,

    -- Crecimiento de ingresos
    CASE
        WHEN cobrado_mes_anterior > 0
        THEN ROUND(100.0 * (total_cobrado - cobrado_mes_anterior) / cobrado_mes_anterior, 2)
    END                                                                 AS mom_revenue_growth_pct,

    CASE
        WHEN cobrado_mismo_mes_anio_anterior > 0
        THEN ROUND(100.0 * (total_cobrado - cobrado_mismo_mes_anio_anterior) / cobrado_mismo_mes_anio_anterior, 2)
    END                                                                 AS yoy_revenue_growth_pct,

    -- KPIs de calidad
    ROUND(100.0 * viajes_con_discrepancia / NULLIF(total_viajes, 0), 2) AS pct_discrepancia_precio,

    -- Ranking dentro del mes por ingresos totales
    RANK() OVER (PARTITION BY anio, mes ORDER BY total_cobrado DESC)    AS rank_revenue_mes

FROM with_lag
ORDER BY anio, mes, total_cobrado DESC
