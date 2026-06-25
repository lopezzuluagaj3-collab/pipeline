{{
  config(
    materialized='materialized_view'
  )
}}

-- ─────────────────────────────────────────────
-- Mart: Operaciones por hora Yellow Taxi
-- Responde: ¿a qué horas hay más demanda?
-- ¿cuál es la hora pico? ¿cuándo baja el servicio?
-- ─────────────────────────────────────────────

WITH fact AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

times AS (
    SELECT * FROM {{ ref('dim_time') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

hourly AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        d.day_name,
        d.is_weekend,
        t.hour,
        t.period_of_day,

        COUNT(*)                                        AS total_trips,
        SUM(f.passenger_count)                          AS total_passengers,
        ROUND(AVG(f.passenger_count), 2)                AS avg_passengers,
        ROUND(AVG(f.trip_distance), 2)                  AS avg_distance_miles,
        ROUND(AVG(f.trip_duration_minutes), 2)          AS avg_duration_min,
        ROUND(SUM(f.total_amt), 2)                      AS total_revenue,
        ROUND(AVG(f.total_amt), 2)                      AS avg_revenue_per_trip

    FROM fact f
    JOIN times t ON f.pickup_time_id = t.time_id
    JOIN dates d ON f.date_id        = d.date_id
    GROUP BY
        d.year, d.month, d.month_name,
        d.day_name, d.is_weekend,
        t.hour, t.period_of_day
)

SELECT * FROM hourly
ORDER BY year, hour;
