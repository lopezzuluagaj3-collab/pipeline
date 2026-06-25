{{
  config(
    materialized='materialized_view'
  )
}}

-- ─────────────────────────────────────────────
-- Mart: Resumen mensual Yellow Taxi
-- Responde: ¿cómo fue cada mes del año?
-- ¿cuál fue el mes con más viajes?
-- ¿cómo evolucionaron los ingresos mes a mes (2009–2026)?
-- ─────────────────────────────────────────────

WITH fact AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

monthly AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        d.quarter,

        COUNT(*)                                                        AS total_trips,
        SUM(f.passenger_count)                                          AS total_passengers,
        ROUND(AVG(f.passenger_count), 2)                                AS avg_passengers,
        ROUND(AVG(f.trip_distance), 2)                                  AS avg_distance_miles,
        ROUND(AVG(f.trip_duration_minutes), 2)                          AS avg_duration_min,
        ROUND(SUM(f.fare_amount), 2)                                    AS total_fare,
        ROUND(SUM(f.extra), 2)                                          AS total_extra,
        ROUND(SUM(f.tip_amt), 2)                                        AS total_tips,
        ROUND(SUM(f.tolls_amt), 2)                                      AS total_tolls,
        ROUND(SUM(f.surcharge), 2)                                      AS total_surcharge,
        ROUND(SUM(f.mta_tax), 2)                                        AS total_mta_tax,
        ROUND(SUM(f.congestion_surcharge), 2)                           AS total_congestion_surcharge,
        ROUND(SUM(f.airport_fee), 2)                                    AS total_airport_fee,
        ROUND(SUM(f.cbd_congestion_fee), 2)                             AS total_cbd_congestion_fee,
        ROUND(SUM(f.total_amt), 2)                                      AS total_revenue,
        ROUND(AVG(f.total_amt), 2)                                      AS avg_revenue_per_trip,
        ROUND(
            SUM(f.tip_amt) / NULLIF(SUM(f.fare_amount), 0) * 100,
        2)                                                              AS tip_pct_of_fare,
        COUNT(CASE WHEN f.comparation_total_amt = FALSE THEN 1 END)     AS trips_with_amt_mismatch

    FROM fact f
    JOIN dates d ON f.date_id = d.date_id
    GROUP BY
        d.year, d.month, d.month_name, d.quarter
)

SELECT * FROM monthly
ORDER BY year, month;
