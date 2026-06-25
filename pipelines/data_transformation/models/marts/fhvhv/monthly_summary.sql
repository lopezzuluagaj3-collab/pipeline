{{ config(
    materialized = 'materialized_view',
    schema       = 'fhvhv_marts'
) }}

/*
  Resumen mensual HVFHS — KPIs globales y por operador.
  Responde: ¿Cómo crece el negocio mes a mes y año a año por operador?
  ¿Cuál es la tendencia del revenue, espera y discrepancias de cobro?
  Métricas: mom/yoy en viajes e ingresos, calidad de cobro, participación de mercado.
*/

WITH monthly AS (

    SELECT
        f.anio,
        f.mes,
        l.nombre_operador,

        COUNT(*)                                                               AS total_viajes,
        COUNT(DISTINCT f.base_key)                                             AS bases_activas,

        -- Métricas operativas
        ROUND(AVG(f.trip_duration_minutes), 2)                                 AS avg_duracion_min,
        ROUND(AVG(f.wait_time_minutes),     2)                                 AS avg_espera_min,
        ROUND(AVG(f.trip_distance),         2)                                 AS avg_distancia_millas,
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.trip_duration_minutes),
            2
        )                                                                      AS mediana_duracion_min,

        -- Revenue
        ROUND(SUM(f.fare_amount),                        2)                    AS total_tarifa_base,
        ROUND(SUM(f.tip_amt),                            2)                    AS total_propinas,
        ROUND(SUM(f.total_amt),                          2)                    AS total_cobrado,
        ROUND(SUM(f.true_total_amt),                     2)                    AS total_ideal,
        ROUND(SUM(f.true_total_amt) - SUM(f.total_amt),  2)                    AS diferencia_cobro_total,
        ROUND(AVG(f.fare_amount),                        2)                    AS avg_tarifa_viaje,
        ROUND(AVG(f.tip_amt),                            2)                    AS avg_propina_viaje,

        -- Calidad de cobro
        COUNT(*) FILTER (WHERE NOT f.comparation_total_amt)                    AS viajes_con_discrepancia,

        -- Servicio compartido y accesibilidad
        COUNT(*) FILTER (WHERE f.shared_request_flag)                          AS viajes_compartidos_solicitados,
        COUNT(*) FILTER (WHERE f.shared_match_flag)                            AS viajes_compartidos_concretados,
        COUNT(*) FILTER (WHERE f.wav_request_flag)                             AS viajes_wav_solicitados,
        COUNT(*) FILTER (WHERE f.wav_match_flag)                               AS viajes_wav_concretados,
        COUNT(*) FILTER (WHERE f.access_a_ride_flag)                           AS viajes_access_a_ride

    FROM {{ ref('fact_hvfhs_trips') }} f
    JOIN {{ ref('dim_hvfhs_license') }} l ON f.license_key = l.license_key
    GROUP BY 1, 2, 3

),

with_lag AS (

    SELECT
        *,
        -- Viajes: lag MoM y YoY
        LAG(total_viajes,     1)  OVER (PARTITION BY nombre_operador ORDER BY anio, mes)
            AS viajes_mes_anterior,
        LAG(total_viajes,    12)  OVER (PARTITION BY nombre_operador ORDER BY anio, mes)
            AS viajes_mismo_mes_anio_anterior,

        -- Revenue: lag MoM y YoY
        LAG(total_cobrado,    1)  OVER (PARTITION BY nombre_operador ORDER BY anio, mes)
            AS cobrado_mes_anterior,
        LAG(total_cobrado,   12)  OVER (PARTITION BY nombre_operador ORDER BY anio, mes)
            AS cobrado_mismo_mes_anio_anterior,

        -- Espera: lag MoM
        LAG(avg_espera_min,   1)  OVER (PARTITION BY nombre_operador ORDER BY anio, mes)
            AS avg_espera_mes_anterior

    FROM monthly

)

SELECT
    *,

    -- Crecimiento de viajes
    CASE
        WHEN viajes_mes_anterior > 0
        THEN ROUND(100.0 * (total_viajes - viajes_mes_anterior) / viajes_mes_anterior, 2)
    END                                                                        AS mom_trip_growth_pct,

    CASE
        WHEN viajes_mismo_mes_anio_anterior > 0
        THEN ROUND(100.0 * (total_viajes - viajes_mismo_mes_anio_anterior) / viajes_mismo_mes_anio_anterior, 2)
    END                                                                        AS yoy_trip_growth_pct,

    -- Crecimiento de revenue
    CASE
        WHEN cobrado_mes_anterior > 0
        THEN ROUND(100.0 * (total_cobrado - cobrado_mes_anterior) / cobrado_mes_anterior, 2)
    END                                                                        AS mom_revenue_growth_pct,

    CASE
        WHEN cobrado_mismo_mes_anio_anterior > 0
        THEN ROUND(100.0 * (total_cobrado - cobrado_mismo_mes_anio_anterior) / cobrado_mismo_mes_anio_anterior, 2)
    END                                                                        AS yoy_revenue_growth_pct,

    -- Cambio en espera
    CASE
        WHEN avg_espera_mes_anterior > 0
        THEN ROUND(100.0 * (avg_espera_min - avg_espera_mes_anterior) / avg_espera_mes_anterior, 2)
    END                                                                        AS mom_avg_wait_change_pct,

    -- KPIs de calidad de cobro
    ROUND(100.0 * viajes_con_discrepancia / NULLIF(total_viajes, 0), 2)        AS pct_discrepancia_precio,
    CASE
        WHEN diferencia_cobro_total > 0  THEN 'Cobro global inferior al ideal'
        WHEN diferencia_cobro_total < 0  THEN 'Cobro global superior al ideal'
        ELSE 'Cobro global exacto'
    END                                                                        AS clasificacion_cobro_mes,

    -- KPIs de servicio
    ROUND(100.0 * viajes_compartidos_concretados / NULLIF(viajes_compartidos_solicitados, 0), 2) AS tasa_match_compartido,
    ROUND(100.0 * viajes_wav_concretados / NULLIF(viajes_wav_solicitados, 0), 2)                 AS tasa_match_wav,

    -- Participación de mercado en el mes
    ROUND(
        100.0 * total_viajes / NULLIF(SUM(total_viajes) OVER (PARTITION BY anio, mes), 0),
        2
    )                                                                          AS market_share_viajes_pct,
    ROUND(
        100.0 * total_cobrado / NULLIF(SUM(total_cobrado) OVER (PARTITION BY anio, mes), 0),
        2
    )                                                                          AS market_share_revenue_pct

FROM with_lag
ORDER BY anio, mes, total_cobrado DESC
