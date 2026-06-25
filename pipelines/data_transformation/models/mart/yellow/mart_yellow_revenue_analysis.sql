{{
  config(
    materialized='materialized_view'
  )
}}

-- ─────────────────────────────────────────────
-- Mart: Análisis de ingresos Yellow Taxi
-- Responde: ¿cuánto se facturó? ¿cómo se compone el cobro?
-- ¿qué método de pago predomina? ¿cómo varía por tarifa?
-- ─────────────────────────────────────────────

WITH fact AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

vendors AS (
    SELECT * FROM {{ ref('dim_vendor') }}
),

payments AS (
    SELECT * FROM {{ ref('dim_payment_type') }}
),

ratecodes AS (
    SELECT * FROM {{ ref('dim_ratecode') }}
),

revenue AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        d.quarter,
        v.company_name                                  AS vendor,
        p.payment_type_name                             AS payment_method,
        r.ratecode_name                                 AS rate_type,

        COUNT(*)                                        AS total_trips,
        ROUND(SUM(f.fare_amount), 2)                    AS total_fare,
        ROUND(SUM(f.extra), 2)                          AS total_extra,
        ROUND(SUM(f.tip_amt), 2)                        AS total_tips,
        ROUND(SUM(f.tolls_amt), 2)                      AS total_tolls,
        ROUND(SUM(f.surcharge), 2)                      AS total_surcharge,
        ROUND(SUM(f.mta_tax), 2)                        AS total_mta_tax,
        ROUND(SUM(f.congestion_surcharge), 2)           AS total_congestion_surcharge,
        ROUND(SUM(f.airport_fee), 2)                    AS total_airport_fee,
        ROUND(SUM(f.cbd_congestion_fee), 2)             AS total_cbd_congestion_fee,
        ROUND(SUM(f.total_amt), 2)                      AS total_revenue,
        ROUND(AVG(f.total_amt), 2)                      AS avg_revenue_per_trip,
        ROUND(
            SUM(f.tip_amt) / NULLIF(SUM(f.fare_amount), 0) * 100,
        2)                                              AS tip_pct_of_fare

    FROM fact f
    JOIN dates     d  ON f.date_id       = d.date_id
    JOIN vendors   v  ON f.vendor_id     = v.vendor_id
    JOIN payments  p  ON f.payment_type  = p.payment_type_id
    JOIN ratecodes r  ON f.ratecode_id   = r.ratecode_id
    GROUP BY
        d.year, d.month, d.month_name, d.quarter,
        v.company_name, p.payment_type_name, r.ratecode_name
)

SELECT * FROM revenue
ORDER BY year, month, total_revenue DESC;
