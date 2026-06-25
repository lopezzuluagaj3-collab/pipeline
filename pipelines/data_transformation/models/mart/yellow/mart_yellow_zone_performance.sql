{{
  config(
    materialized='materialized_view'
  )
}}

-- ─────────────────────────────────────────────
-- Mart: Desempeño por zonas Yellow Taxi
-- Responde: ¿desde qué zonas salen más viajes?
-- ¿hacia dónde van? ¿qué corredores generan más ingreso?
-- ─────────────────────────────────────────────

WITH fact AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

locations AS (
    SELECT * FROM {{ ref('dim_location') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

zone_perf AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        l_pu.borough        AS pickup_borough,
        l_pu.zone           AS pickup_zone,
        l_pu.service_zone   AS pickup_service_zone,
        l_do.borough        AS dropoff_borough,
        l_do.zone           AS dropoff_zone,
        l_do.service_zone   AS dropoff_service_zone,

        COUNT(*)                                        AS total_trips,
        ROUND(AVG(f.trip_distance), 2)                  AS avg_distance_miles,
        ROUND(AVG(f.trip_duration_minutes), 2)          AS avg_duration_min,
        ROUND(SUM(f.total_amt), 2)                      AS total_revenue,
        ROUND(AVG(f.total_amt), 2)                      AS avg_revenue_per_trip,
        ROUND(SUM(f.tip_amt), 2)                        AS total_tips,
        ROUND(AVG(f.tip_amt), 2)                        AS avg_tip

    FROM fact f
    JOIN locations l_pu ON f.pu_location_id = l_pu.location_id
    JOIN locations l_do ON f.do_location_id = l_do.location_id
    JOIN dates     d    ON f.date_id         = d.date_id
    GROUP BY
        d.year, d.month, d.month_name,
        l_pu.borough, l_pu.zone, l_pu.service_zone,
        l_do.borough, l_do.zone, l_do.service_zone
)

SELECT * FROM zone_perf
ORDER BY total_trips DESC;
