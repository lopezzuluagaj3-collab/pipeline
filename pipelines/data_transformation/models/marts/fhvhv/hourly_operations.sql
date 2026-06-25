{{ config(
    materialized = 'materialized_view',
    schema       = 'fhvhv_marts'
) }}

/*
  Operaciones por franja horaria HVFHS.
  Responde: ¿Cómo afectan las horas pico a la demanda, espera y tarifa?
  ¿Qué operador domina cada franja horaria?
*/

WITH base AS (

    SELECT
        f.anio,
        f.mes,
        f.pickup_hour,
        l.nombre_operador,

        CASE
            WHEN f.pickup_hour BETWEEN 7  AND 9  THEN 'Hora pico mañana'
            WHEN f.pickup_hour BETWEEN 17 AND 19 THEN 'Hora pico tarde'
            WHEN f.pickup_hour BETWEEN 0  AND 5  THEN 'Madrugada'
            WHEN f.pickup_hour BETWEEN 10 AND 16 THEN 'Horario diurno'
            ELSE 'Horario nocturno'
        END                                                        AS franja_horaria,

        COUNT(*)                                                   AS total_viajes,
        ROUND(AVG(f.wait_time_minutes),      2)                    AS avg_espera_min,
        ROUND(AVG(f.trip_duration_minutes),  2)                    AS avg_duracion_min,
        ROUND(AVG(f.trip_distance),          2)                    AS avg_distancia_millas,
        ROUND(AVG(f.fare_amount),            2)                    AS avg_tarifa_base,
        ROUND(AVG(f.tip_amt),                2)                    AS avg_propina,
        ROUND(SUM(f.total_amt),              2)                    AS total_cobrado,
        ROUND(SUM(f.true_total_amt),         2)                    AS total_ideal,
        ROUND(SUM(f.true_total_amt) - SUM(f.total_amt), 2)        AS diferencia_cobro,

        COUNT(*) FILTER (WHERE f.shared_request_flag)              AS viajes_compartidos_solicitados,
        COUNT(*) FILTER (WHERE f.shared_match_flag)                AS viajes_compartidos_concretados,
        COUNT(*) FILTER (WHERE NOT f.comparation_total_amt)        AS viajes_con_discrepancia_precio

    FROM {{ ref('fact_hvfhs_trips') }} f
    JOIN {{ ref('dim_hvfhs_license') }} l ON f.license_key = l.license_key
    GROUP BY 1, 2, 3, 4, 5

)

SELECT
    *,
    ROUND(
        100.0 * total_viajes / NULLIF(SUM(total_viajes) OVER (PARTITION BY anio, mes, nombre_operador), 0),
        2
    )                                                              AS pct_viajes_del_mes,
    ROUND(
        100.0 * total_viajes / NULLIF(SUM(total_viajes) OVER (PARTITION BY anio, mes, franja_horaria), 0),
        2
    )                                                              AS pct_viajes_en_franja,
    ROUND(
        100.0 * viajes_con_discrepancia_precio / NULLIF(total_viajes, 0),
        2
    )                                                              AS pct_discrepancia_precio,
    -- Ranking del operador dentro de cada hora
    RANK() OVER (PARTITION BY anio, mes, pickup_hour ORDER BY total_viajes DESC) AS rank_operador_en_hora
FROM base
ORDER BY anio, mes, pickup_hour, nombre_operador
