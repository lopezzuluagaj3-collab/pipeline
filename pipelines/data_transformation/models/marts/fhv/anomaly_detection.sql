{{ config(
    materialized = 'materialized_view',
    schema       = 'fhv_marts'
) }}

/*
  Detección de anomalías en viajes FHV.
  Responde: ¿Qué viajes tienen duraciones estadísticamente atípicas?
  Metodología: rango intercuartílico (IQR) — umbral Tukey 1.5×IQR.
  Dado que FHV no tiene datos financieros, el análisis se centra en la duración.
*/

WITH stats_mensuales AS (

    SELECT
        anio,
        mes,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_duration_minutes) AS p25_dur,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_duration_minutes) AS p75_dur,
        AVG(trip_duration_minutes)                                           AS avg_dur,
        STDDEV(trip_duration_minutes)                                        AS std_dur
    FROM {{ ref('fact_fhv_trips') }}
    WHERE trip_duration_minutes > 0
    GROUP BY 1, 2

),

viajes_con_stats AS (

    SELECT
        f.trip_id,
        f.pickup_datetime,
        f.dropoff_datetime,
        f.anio,
        f.mes,
        f.pickup_hour,
        f.trip_duration_minutes,
        b.dispatching_base_num,
        ab.affiliated_base_number,
        s.p25_dur,
        s.p75_dur,
        s.avg_dur,
        s.std_dur,
        (s.p75_dur - s.p25_dur)                          AS iqr_dur,
        s.p25_dur - 1.5 * (s.p75_dur - s.p25_dur)       AS limite_inferior,
        s.p75_dur + 1.5 * (s.p75_dur - s.p25_dur)       AS limite_superior

    FROM {{ ref('fact_fhv_trips') }} f
    JOIN {{ ref('dim_fhv_base') }}          b  ON f.base_key          = b.base_key
    JOIN {{ ref('dim_fhv_affiliated_base') }} ab ON f.affiliated_base_key = ab.affiliated_base_key
    JOIN stats_mensuales                    s  ON f.anio = s.anio AND f.mes = s.mes
    WHERE f.trip_duration_minutes > 0

),

final AS (

    SELECT
        trip_id,
        dispatching_base_num,
        affiliated_base_number,
        pickup_datetime,
        dropoff_datetime,
        pickup_hour,
        trip_duration_minutes,
        anio,
        mes,
        ROUND(avg_dur, 2)           AS avg_duracion_mes,
        ROUND(limite_inferior, 2)   AS limite_inferior_tukey,
        ROUND(limite_superior, 2)   AS limite_superior_tukey,

        CASE
            WHEN trip_duration_minutes < limite_inferior THEN 'Duración anormalmente corta'
            WHEN trip_duration_minutes > limite_superior THEN 'Duración anormalmente larga'
        END                         AS tipo_anomalia,

        ROUND(
            (trip_duration_minutes - avg_dur) / NULLIF(std_dur, 0),
            2
        )                           AS z_score_duracion

    FROM viajes_con_stats
    WHERE trip_duration_minutes < limite_inferior
       OR trip_duration_minutes > limite_superior

)

SELECT * FROM final
ORDER BY ABS(z_score_duracion) DESC
