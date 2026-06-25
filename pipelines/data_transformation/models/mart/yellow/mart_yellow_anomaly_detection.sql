{{
  config(
    materialized='materialized_view'
  )
}}

-- ─────────────────────────────────────────────
-- Mart: Detección de anomalías Yellow Taxi
-- Responde: ¿hay viajes con distancia cero?
-- ¿tarifas negativas? ¿duraciones imposibles?
-- ¿total cobrado que no cuadra con los componentes?
-- ─────────────────────────────────────────────

WITH fact AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

anomalies AS (
    SELECT
        d.full_date,
        d.year,
        d.month,
        d.month_name,
        f.vendor_id,
        f.tpep_pickup_datetime,
        f.tpep_dropoff_datetime,
        f.trip_distance,
        f.trip_duration_minutes,
        f.passenger_count,
        f.fare_amount,
        f.total_amt,
        f.true_total_amt,
        f.comparation_total_amt,
        f.pu_location_id,
        f.do_location_id,
        f.ratecode_id,
        f.payment_type,

        CASE WHEN f.trip_distance = 0                   THEN TRUE ELSE FALSE END AS zero_distance,
        CASE WHEN f.fare_amount < 0                     THEN TRUE ELSE FALSE END AS negative_fare,
        CASE WHEN f.total_amt < 0                       THEN TRUE ELSE FALSE END AS negative_total,
        CASE WHEN f.trip_duration_minutes <= 0          THEN TRUE ELSE FALSE END AS zero_or_negative_duration,
        CASE WHEN f.trip_duration_minutes > 300         THEN TRUE ELSE FALSE END AS extreme_duration,
        CASE WHEN f.trip_distance > 100                 THEN TRUE ELSE FALSE END AS extreme_distance,
        CASE WHEN f.passenger_count > 6                 THEN TRUE ELSE FALSE END AS excess_passengers,
        CASE WHEN f.comparation_total_amt = FALSE       THEN TRUE ELSE FALSE END AS total_amt_mismatch

    FROM fact f
    JOIN dates d ON f.date_id = d.date_id
    WHERE
        f.trip_distance = 0
        OR f.fare_amount < 0
        OR f.total_amt < 0
        OR f.trip_duration_minutes <= 0
        OR f.trip_duration_minutes > 300
        OR f.trip_distance > 100
        OR f.passenger_count > 6
        OR f.comparation_total_amt = FALSE
)

SELECT * FROM anomalies
ORDER BY full_date;
