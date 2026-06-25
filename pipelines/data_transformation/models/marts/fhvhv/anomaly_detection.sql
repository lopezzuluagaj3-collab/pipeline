{{ config(
    materialized = 'materialized_view',
    schema       = 'fhvhv_marts'
) }}

/*
  Detección de anomalías HVFHS — precio cobrado vs ideal y outliers operativos.
  Responde: ¿Qué viajes tienen discrepancias en el cobro? ¿Son por exceso o defecto?
  ¿Dónde ocurren? ¿Qué operador/base genera más anomalías?
  Metodología: IQR Tukey para outliers de tarifa/duración/espera + flag de precio.
*/

WITH stats_mensuales AS (

    SELECT
        anio,
        mes,
        -- Percentiles para tarifa base
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY fare_amount)          AS fare_p25,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY fare_amount)          AS fare_p75,
        -- Percentiles para duración
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_duration_minutes) AS dur_p25,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_duration_minutes) AS dur_p75,
        -- Percentiles para tiempo de espera
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY wait_time_minutes)    AS wait_p25,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY wait_time_minutes)    AS wait_p75,

        AVG(fare_amount)           AS avg_fare,
        STDDEV(fare_amount)        AS std_fare,
        AVG(trip_duration_minutes) AS avg_dur,
        STDDEV(trip_duration_minutes) AS std_dur

    FROM {{ ref('fact_hvfhs_trips') }}
    WHERE fare_amount > 0
      AND trip_duration_minutes > 0
      AND wait_time_minutes >= 0
    GROUP BY 1, 2

),

viajes_clasificados AS (

    SELECT
        f.trip_id,
        f.pickup_datetime,
        f.dropoff_datetime,
        f.anio,
        f.mes,
        f.pickup_hour,
        l.nombre_operador,
        b.dispatching_base_num,
        pu.zone                                                              AS zona_origen,
        pu.borough                                                           AS borough_origen,
        dol.zone                                                             AS zona_destino,

        -- Métricas del viaje
        f.trip_distance,
        f.trip_duration_minutes,
        f.wait_time_minutes,
        f.fare_amount,
        f.total_amt,
        f.true_total_amt,
        f.comparation_total_amt,
        ROUND(f.true_total_amt - f.total_amt, 2)                             AS diferencia_precio,

        -- Límites IQR tarifa
        s.fare_p25 - 1.5 * (s.fare_p75 - s.fare_p25)                       AS fare_limite_inf,
        s.fare_p75 + 1.5 * (s.fare_p75 - s.fare_p25)                       AS fare_limite_sup,
        -- Límites IQR duración
        s.dur_p25  - 1.5 * (s.dur_p75 - s.dur_p25)                         AS dur_limite_inf,
        s.dur_p75  + 1.5 * (s.dur_p75 - s.dur_p25)                         AS dur_limite_sup,
        -- Límites IQR espera
        s.wait_p75 + 1.5 * (s.wait_p75 - s.wait_p25)                       AS wait_limite_sup,

        -- Z-scores
        ROUND((f.fare_amount - s.avg_fare) / NULLIF(s.std_fare, 0), 2)      AS z_score_tarifa,
        ROUND((f.trip_duration_minutes - s.avg_dur) / NULLIF(s.std_dur, 0), 2) AS z_score_duracion,

        -- Clasificación de anomalía de precio (primera prioridad)
        CASE
            WHEN NOT f.comparation_total_amt AND f.true_total_amt - f.total_amt > 0
                THEN 'Precio cobrado inferior al ideal'
            WHEN NOT f.comparation_total_amt AND f.true_total_amt - f.total_amt < 0
                THEN 'Precio cobrado superior al ideal'
            WHEN f.fare_amount < s.fare_p25 - 1.5 * (s.fare_p75 - s.fare_p25)
                THEN 'Tarifa anormalmente baja'
            WHEN f.fare_amount > s.fare_p75 + 1.5 * (s.fare_p75 - s.fare_p25)
                THEN 'Tarifa anormalmente alta'
            WHEN f.trip_duration_minutes < s.dur_p25 - 1.5 * (s.dur_p75 - s.dur_p25)
                THEN 'Duración anormalmente corta'
            WHEN f.trip_duration_minutes > s.dur_p75 + 1.5 * (s.dur_p75 - s.dur_p25)
                THEN 'Duración anormalmente larga'
            WHEN f.wait_time_minutes > s.wait_p75 + 1.5 * (s.wait_p75 - s.wait_p25)
                THEN 'Espera excesiva'
        END                                                                  AS tipo_anomalia

    FROM {{ ref('fact_hvfhs_trips') }} f
    JOIN {{ ref('dim_hvfhs_license') }}  l   ON f.license_key    = l.license_key
    JOIN {{ ref('dim_hvfhs_base') }}     b   ON f.base_key       = b.base_key
    LEFT JOIN {{ ref('dim_hvfhs_location') }} pu  ON f.pu_location_id = pu.location_id
    LEFT JOIN {{ ref('dim_hvfhs_location') }} dol ON f.do_location_id = dol.location_id
    JOIN stats_mensuales                 s   ON f.anio = s.anio AND f.mes = s.mes
    WHERE f.fare_amount > 0
      AND f.trip_duration_minutes > 0

)

SELECT * FROM viajes_clasificados
WHERE tipo_anomalia IS NOT NULL
ORDER BY anio, mes, ABS(diferencia_precio) DESC NULLS LAST
