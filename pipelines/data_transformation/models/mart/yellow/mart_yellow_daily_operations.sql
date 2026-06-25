{{
  config(
    materialized='materialized_view'
  )
}}

-- ─────────────────────────────────────────────
-- Mart: Operaciones diarias Yellow Taxi
-- Responde: ¿cuántos viajes hubo cada día?
-- ¿cuánto duró el promedio? ¿cuántos pasajeros?
-- ─────────────────────────────────────────────

WITH fact AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

daily AS (
    SELECT
        d.full_date,
        d.year,
        d.month,
        d.month_name,
        d.day_of_week,
        d.day_name,
        d.week_of_year,
        d.quarter,
        d.is_weekend,

        COUNT(*)                                        AS total_trips,
        SUM(f.passenger_count)                          AS total_passengers,
        ROUND(AVG(f.passenger_count), 2)                AS avg_passengers,
        ROUND(AVG(f.trip_distance), 2)                  AS avg_distance_miles,
        ROUND(AVG(f.trip_duration_minutes), 2)          AS avg_duration_min,
        ROUND(SUM(f.total_amt), 2)                      AS total_revenue,
        ROUND(AVG(f.total_amt), 2)                      AS avg_revenue_per_trip,
        ROUND(SUM(f.tip_amt), 2)                        AS total_tips,
        ROUND(AVG(f.tip_amt), 2)                        AS avg_tip

    FROM fact f
    JOIN dates d ON f.date_id = d.date_id
    GROUP BY
        d.full_date, d.year, d.month, d.month_name,
        d.day_of_week, d.day_name, d.week_of_year,
        d.quarter, d.is_weekend
)

SELECT * FROM daily
ORDER BY full_date;
