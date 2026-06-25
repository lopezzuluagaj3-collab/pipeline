{{ config(
    materialized = 'materialized_view',
    schema       = 'fhvhv_marts'
) }}

/*
  Operaciones diarias HVFHS por operador.
  Responde: ¿Cuántos viajes y qué ingresos se generan por día y operador?
  ¿Cuál es la tasa de discrepancia de precio (cobrado vs ideal)?
  ¿Cómo varía la espera promedio según el día?
*/

WITH trips_enriquecidos AS (

    SELECT
        f.pickup_date_key,
        f.anio,
        f.mes,
        f.trip_duration_minutes,
        f.wait_time_minutes,
        f.trip_distance,
        f.fare_amount,
        f.tip_amt,
        f.total_amt,
        f.true_total_amt,
        f.comparation_total_amt,
        f.shared_request_flag,
        f.shared_match_flag,
        f.wav_request_flag,
        f.wav_match_flag,
        f.base_key,
        l.nombre_operador,
        d.trip_date,
        d.nombre_dia,
        d.dia_semana_iso,
        d.trimestre,
        d.es_fin_de_semana
    FROM {{ ref('fact_hvfhs_trips') }} f
    JOIN {{ ref('dim_hvfhs_license') }} l ON f.license_key    = l.license_key
    JOIN {{ ref('dim_hvfhs_date') }}    d ON f.pickup_date_key = d.date_key

),

daily_agg AS (

    SELECT
        trip_date,
        anio,
        mes,
        nombre_dia,
        dia_semana_iso,
        trimestre,
        es_fin_de_semana,
        nombre_operador,

        COUNT(*)                                                               AS total_viajes,
        COUNT(DISTINCT base_key)                                               AS bases_activas,

        -- Métricas operativas
        ROUND(AVG(trip_duration_minutes), 2)                                   AS avg_duracion_min,
        ROUND(AVG(wait_time_minutes),     2)                                   AS avg_espera_min,
        ROUND(AVG(trip_distance),         2)                                   AS avg_distancia_millas,

        -- Revenue: precio cobrado vs ideal
        ROUND(SUM(fare_amount),                         2)                     AS total_tarifa_base,
        ROUND(SUM(total_amt),                           2)                     AS total_cobrado,
        ROUND(SUM(true_total_amt),                      2)                     AS total_ideal,
        ROUND(SUM(true_total_amt) - SUM(total_amt),     2)                     AS diferencia_cobro,
        ROUND(AVG(fare_amount),                         2)                     AS avg_tarifa_viaje,
        ROUND(AVG(tip_amt),                             2)                     AS avg_propina,

        -- Flags de servicio
        COUNT(*) FILTER (WHERE shared_request_flag)                            AS viajes_compartidos_solicitados,
        COUNT(*) FILTER (WHERE shared_match_flag)                              AS viajes_compartidos_concretados,
        COUNT(*) FILTER (WHERE wav_request_flag)                               AS viajes_wav_solicitados,
        COUNT(*) FILTER (WHERE wav_match_flag)                                 AS viajes_wav_concretados,

        -- Anomalías de precio
        COUNT(*) FILTER (WHERE NOT comparation_total_amt)                      AS viajes_con_discrepancia_precio

    FROM trips_enriquecidos
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

)

SELECT
    *,
    ROUND(100.0 * viajes_con_discrepancia_precio / NULLIF(total_viajes, 0), 2)           AS pct_discrepancia_precio,
    ROUND(100.0 * viajes_compartidos_concretados / NULLIF(viajes_compartidos_solicitados, 0), 2) AS tasa_match_compartido,
    ROUND(100.0 * viajes_wav_concretados / NULLIF(viajes_wav_solicitados, 0), 2)          AS tasa_match_wav,
    CASE
        WHEN diferencia_cobro > 0  THEN 'Cobro inferior al ideal'
        WHEN diferencia_cobro < 0  THEN 'Cobro superior al ideal'
        ELSE 'Cobro exacto'
    END                                                                                    AS clasificacion_cobro
FROM daily_agg
ORDER BY trip_date, nombre_operador
