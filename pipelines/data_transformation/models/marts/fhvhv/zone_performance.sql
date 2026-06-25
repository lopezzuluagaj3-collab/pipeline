{{ config(
    materialized = 'materialized_view',
    schema       = 'fhvhv_marts'
) }}

/*
  Rendimiento mensual por zona de origen HVFHS.
  Responde: ¿Qué zonas generan más ingresos o pérdidas?
  ¿Cuáles son los corredores origen-destino más rentables?
  ¿Dónde se concentran las discrepancias de precio?
*/

WITH zone_monthly AS (

    SELECT
        f.anio,
        f.mes,
        l.nombre_operador,

        -- Zona de origen
        pu.location_id                                                       AS pu_location_id,
        pu.borough                                                           AS borough_origen,
        pu.zone                                                              AS zona_origen,
        pu.service_zone                                                      AS service_zone_origen,

        -- Zona de destino
        dol.location_id                                                      AS do_location_id,
        dol.borough                                                          AS borough_destino,
        dol.zone                                                             AS zona_destino,

        COUNT(*)                                                             AS total_viajes,
        ROUND(AVG(f.trip_distance),         2)                               AS avg_distancia_millas,
        ROUND(AVG(f.trip_duration_minutes), 2)                               AS avg_duracion_min,
        ROUND(AVG(f.wait_time_minutes),     2)                               AS avg_espera_min,

        -- Revenue
        ROUND(AVG(f.fare_amount),                       2)                   AS avg_tarifa_base,
        ROUND(SUM(f.fare_amount),                       2)                   AS total_tarifa_base,
        ROUND(SUM(f.total_amt),                         2)                   AS total_cobrado,
        ROUND(SUM(f.true_total_amt),                    2)                   AS total_ideal,
        ROUND(SUM(f.true_total_amt) - SUM(f.total_amt), 2)                   AS diferencia_cobro,
        ROUND(AVG(f.tip_amt),                           2)                   AS avg_propina,

        -- Eficiencia por milla
        ROUND(
            SUM(f.fare_amount) / NULLIF(SUM(f.trip_distance), 0),
            2
        )                                                                    AS tarifa_por_milla,

        -- Calidad de cobro
        COUNT(*) FILTER (WHERE NOT f.comparation_total_amt)                  AS viajes_con_discrepancia,

        -- Viajes compartidos
        COUNT(*) FILTER (WHERE f.shared_match_flag)                          AS viajes_compartidos

    FROM {{ ref('fact_hvfhs_trips') }} f
    JOIN {{ ref('dim_hvfhs_license') }}  l   ON f.license_key    = l.license_key
    LEFT JOIN {{ ref('dim_hvfhs_location') }} pu  ON f.pu_location_id = pu.location_id
    LEFT JOIN {{ ref('dim_hvfhs_location') }} dol ON f.do_location_id = dol.location_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

)

SELECT
    *,
    ROUND(100.0 * viajes_con_discrepancia / NULLIF(total_viajes, 0), 2)     AS pct_discrepancia_precio,
    ROUND(100.0 * viajes_compartidos      / NULLIF(total_viajes, 0), 2)     AS pct_viajes_compartidos,

    -- Clasificación de rentabilidad de la zona de origen
    CASE
        WHEN diferencia_cobro > 0  THEN 'Zona con cobro inferior al ideal'
        WHEN diferencia_cobro < 0  THEN 'Zona con cobro superior al ideal'
        ELSE 'Zona con cobro exacto'
    END                                                                      AS clasificacion_cobro,

    -- Ranking de zona de origen por ingresos en el mes
    RANK() OVER (
        PARTITION BY anio, mes, nombre_operador
        ORDER BY total_cobrado DESC
    )                                                                        AS rank_zona_origen_revenue

FROM zone_monthly
ORDER BY anio, mes, nombre_operador, total_cobrado DESC
