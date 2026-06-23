{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/staging/fhvhv/anio=' ~ var("anio") ~ '/mes=' ~ "%02d" | format(var("mes") | int),
    pre_hook=[
      "SET spark.sql.shuffle.partitions = 600",
      "SET spark.sql.adaptive.enabled = true",
      "SET spark.sql.adaptive.coalescePartitions.enabled = true",
      "CREATE OR REPLACE TEMPORARY VIEW fhvhv_source AS SELECT * EXCEPT(cbd_congestion_fee), {% if var('anio') | int < 2025 %}CAST(0.0 AS DOUBLE){% else %}CAST(cbd_congestion_fee AS DOUBLE){% endif %} AS cbd_congestion_fee FROM parquet.`s3a://sirius-logs-riwi/tlc/raw/fhvhv/" ~ var("anio") ~ "/fhvhv_tripdata_" ~ var("anio") ~ "-" ~ "%02d" | format(var("mes") | int) ~ ".parquet`"
    ]
  )
}}

-- 1. Read from the temporary view green_source
WITH raw_data AS (
  SELECT *
  FROM green_source
),

-- 2. Rename columns to a normalized name
normalized_columns AS (
    SELECT
        CAST(VendorID AS INTEGER)                AS vendor_id,
        CAST(lpep_pickup_datetime AS TIMESTAMP)  AS tpep_pickup_datetime,
        CAST(lpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
        CAST(RatecodeID AS INTEGER)              AS ratecode_id,
        CAST(PULocationID AS INTEGER)            AS pu_location_id,
        CAST(DOLocationID AS INTEGER)            AS do_location_id,
        CAST(passenger_count AS INTEGER)         AS passenger_count,
        CAST(trip_distance AS DOUBLE)            AS trip_distance,
        CAST(fare_amount AS DOUBLE)              AS fare_amount,
        CAST(extra AS DOUBLE)                    AS extra,
        CAST(mta_tax AS DOUBLE)                  AS mta_tax,
        CAST(tip_amount AS DOUBLE)               AS tip_amt,
        CAST(tolls_amount AS DOUBLE)             AS tolls_amt,
        CAST(congestion_surcharge AS DOUBLE)     AS congestion_surcharge,
        CAST(cbd_congestion_fee AS DOUBLE)       AS cbd_congestion_fee,
        CAST(improvement_surcharge AS DOUBLE)    AS improvement_surcharge,
        CAST(total_amount AS DOUBLE)             AS total_amt,
        CAST(payment_type AS INTEGER)            AS payment_type,
        CAST(trip_type AS INTEGER)               AS trip_type,

        -- Columnas derivadas calculadas aqui para disponibilidad en CTEs posteriores
        COALESCE(CAST(fare_amount AS DOUBLE), 0) +
        COALESCE(CAST(extra AS DOUBLE), 0) +
        COALESCE(CAST(mta_tax AS DOUBLE), 0) +
        COALESCE(CAST(tip_amount AS DOUBLE), 0) +
        COALESCE(CAST(tolls_amount AS DOUBLE), 0) +
        COALESCE(CAST(congestion_surcharge AS DOUBLE), 0) +
        COALESCE(CAST(cbd_congestion_fee AS DOUBLE), 0) +
        COALESCE(CAST(improvement_surcharge AS DOUBLE), 0) AS true_total_amt,

        COALESCE(CAST(total_amount AS DOUBLE), 0) - (
            COALESCE(CAST(fare_amount AS DOUBLE), 0) +
            COALESCE(CAST(extra AS DOUBLE), 0) +
            COALESCE(CAST(mta_tax AS DOUBLE), 0) +
            COALESCE(CAST(tip_amount AS DOUBLE), 0) +
            COALESCE(CAST(tolls_amount AS DOUBLE), 0) +
            COALESCE(CAST(congestion_surcharge AS DOUBLE), 0) +
            COALESCE(CAST(cbd_congestion_fee AS DOUBLE), 0) +
            COALESCE(CAST(improvement_surcharge AS DOUBLE), 0)
        ) AS comparation_total_amt

    FROM raw_data
),

-- 3. Filter anomalies in dates
correct_date AS (
    SELECT *
    FROM normalized_columns
    WHERE EXTRACT(year FROM tpep_pickup_datetime) = {{ var("anio") }}
    AND   EXTRACT(year FROM tpep_dropoff_datetime) = {{ var("anio") }}
),

-- 4. Delete duplicates
duplicated_delete AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY
                       vendor_id,
                       tpep_pickup_datetime,
                       tpep_dropoff_datetime,
                       pu_location_id,
                       do_location_id
                   ORDER BY tpep_pickup_datetime
               ) AS rn
        FROM correct_date
    )
    WHERE rn = 1
),

-- 5. Delete anomalies in trip distance and total amount
delete_amount_anomalies AS (
    SELECT *
    FROM duplicated_delete
    WHERE total_amt >= 0
    AND   trip_distance >= 0
),

-- 6. Delete date anomalies
delete_date_anomalies AS (
    SELECT *
    FROM delete_amount_anomalies
    WHERE tpep_dropoff_datetime >= tpep_pickup_datetime
),

-- 7.1 First null filter: ratecode_id and amount columns
nulls_first_filter AS (
    SELECT
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        COALESCE(ratecode_id, 1)            AS ratecode_id,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        COALESCE(fare_amount, 0)            AS fare_amount,
        COALESCE(extra, 0)                  AS extra,
        COALESCE(mta_tax, 0)               AS mta_tax,
        COALESCE(tip_amt, 0)               AS tip_amt,
        COALESCE(tolls_amt, 0)             AS tolls_amt,
        COALESCE(congestion_surcharge, 0)  AS congestion_surcharge,
        COALESCE(cbd_congestion_fee, 0)    AS cbd_congestion_fee,
        COALESCE(improvement_surcharge, 0) AS improvement_surcharge,
        total_amt,
        true_total_amt,
        comparation_total_amt,
        payment_type,
        COALESCE(trip_type, 1)             AS trip_type
    FROM delete_date_anomalies
),

-- 7.2 Second null filter: passenger_count fixing
stats_passenger AS (
    SELECT FLOOR(AVG(passenger_count)) AS passenger_count_avg
    FROM nulls_first_filter
    WHERE passenger_count IS NOT NULL
    AND   passenger_count <= 6
),

nulls_second_filter AS (
    SELECT /*+ BROADCAST(s) */
        n.vendor_id,
        n.tpep_pickup_datetime,
        n.tpep_dropoff_datetime,
        n.ratecode_id,
        n.pu_location_id,
        n.do_location_id,
        n.trip_distance,
        n.fare_amount,
        n.extra,
        n.mta_tax,
        n.tip_amt,
        n.tolls_amt,
        n.congestion_surcharge,
        n.cbd_congestion_fee,
        n.improvement_surcharge,
        n.total_amt,
        n.true_total_amt,
        n.comparation_total_amt,
        n.payment_type,
        n.trip_type,
        CASE
            WHEN n.passenger_count IS NULL THEN s.passenger_count_avg
            WHEN n.passenger_count > 6     THEN 6
            ELSE n.passenger_count
        END AS passenger_count
    FROM nulls_first_filter n
    CROSS JOIN stats_passenger s
),

-- 7.3 Third null filter: payment_type fixing
range_payment AS (
    SELECT
        payment_type,
        proportion,
        SUM(proportion) OVER (ORDER BY payment_type) - proportion AS low_limit,
        SUM(proportion) OVER (ORDER BY payment_type)              AS upper_limit
    FROM (
        SELECT
            payment_type,
            COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS proportion
        FROM nulls_second_filter
        WHERE payment_type IS NOT NULL
        GROUP BY payment_type
    )
),

nulls_third_filter AS (
    SELECT /*+ BROADCAST(r) */
        n.vendor_id,
        n.tpep_pickup_datetime,
        n.tpep_dropoff_datetime,
        n.ratecode_id,
        n.pu_location_id,
        n.do_location_id,
        n.passenger_count,
        n.trip_distance,
        n.fare_amount,
        n.extra,
        n.mta_tax,
        n.tip_amt,
        n.tolls_amt,
        n.congestion_surcharge,
        n.cbd_congestion_fee,
        n.improvement_surcharge,
        n.total_amt,
        n.true_total_amt,
        n.comparation_total_amt,
        n.trip_type,
        COALESCE(n.payment_type, r.payment_type) AS payment_type
    FROM (
        SELECT *, RAND() AS rand_val
        FROM nulls_second_filter
    ) n
    LEFT JOIN range_payment r
        ON n.payment_type IS NULL
        AND n.rand_val >= r.low_limit
        AND n.rand_val <  r.upper_limit
),

-- 8. Outliers management
stats_percentiles AS (
    SELECT
        approx_percentile(trip_distance,         0.99, 10000) AS p99_trip_distance,
        approx_percentile(fare_amount,           0.99, 10000) AS p99_fare_amount,
        approx_percentile(extra,                 0.99, 10000) AS p99_extra,
        approx_percentile(mta_tax,               0.99, 10000) AS p99_mta_tax,
        approx_percentile(tip_amt,               0.99, 10000) AS p99_tip_amt,
        approx_percentile(tolls_amt,             0.99, 10000) AS p99_tolls_amt,
        approx_percentile(congestion_surcharge,  0.99, 10000) AS p99_congestion_surcharge,
        approx_percentile(cbd_congestion_fee,    0.99, 10000) AS p99_cbd_congestion_fee,
        approx_percentile(improvement_surcharge, 0.99, 10000) AS p99_improvement_surcharge,
        approx_percentile(total_amt,             0.99, 10000) AS p99_total_amt,
        approx_percentile(true_total_amt,        0.99, 10000) AS p99_true_total_amt
    FROM nulls_third_filter
),

calculated_max AS (
    SELECT
        p99_trip_distance         * 1.5 AS max_trip_distance,
        p99_fare_amount           * 1.5 AS max_fare_amount,
        p99_extra                 * 1.5 AS max_extra,
        p99_mta_tax               * 1.5 AS max_mta_tax,
        p99_tip_amt               * 1.5 AS max_tip_amt,
        p99_tolls_amt             * 1.5 AS max_tolls_amt,
        p99_congestion_surcharge  * 1.5 AS max_congestion_surcharge,
        p99_cbd_congestion_fee    * 1.5 AS max_cbd_congestion_fee,
        p99_improvement_surcharge * 1.5 AS max_improvement_surcharge,
        p99_total_amt             * 1.5 AS max_total_amt,
        p99_true_total_amt        * 1.5 AS max_true_total_amt
    FROM stats_percentiles
),

atypical_amount_values_fixing AS (
    SELECT /*+ BROADCAST(c) */
        n.vendor_id,
        n.tpep_pickup_datetime,
        n.tpep_dropoff_datetime,
        n.ratecode_id,
        n.pu_location_id,
        n.do_location_id,
        n.passenger_count,
        n.comparation_total_amt,
        n.congestion_surcharge,
        n.payment_type,
        n.trip_type,
        n.mta_tax,
        n.extra,
        n.cbd_congestion_fee,
        n.improvement_surcharge,
        CASE WHEN n.trip_distance > c.max_trip_distance                   THEN c.max_trip_distance                   ELSE n.trip_distance                   END AS trip_distance,
        CASE WHEN n.fare_amount > c.max_fare_amount                       THEN c.max_fare_amount                     ELSE n.fare_amount                     END AS fare_amount,
        CASE WHEN n.tip_amt > c.max_tip_amt                               THEN c.max_tip_amt                         ELSE n.tip_amt                         END AS tip_amt,
        CASE WHEN n.tolls_amt > c.max_tolls_amt                           THEN c.max_tolls_amt                       ELSE n.tolls_amt                       END AS tolls_amt,
        CASE WHEN n.total_amt > c.max_total_amt                           THEN c.max_total_amt                       ELSE n.total_amt                       END AS total_amt,
        CASE WHEN n.true_total_amt > c.max_true_total_amt                 THEN c.max_true_total_amt                  ELSE n.true_total_amt                  END AS true_total_amt,
        -- Columnas de particion
        CAST({{ var("anio") }} AS INTEGER) AS anio,
        CAST({{ var("mes") }}  AS INTEGER) AS mes
    FROM nulls_third_filter n
    CROSS JOIN calculated_max c
)

SELECT * FROM atypical_amount_values_fixing
