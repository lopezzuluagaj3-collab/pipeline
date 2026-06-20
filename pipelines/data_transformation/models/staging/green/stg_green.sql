{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/staging/green/anio={{ var("anio") }}/mes={{ "%02d" | format(var("mes") | int) }}',
    pre_hook=[
      "CREATE OR REPLACE TEMPORARY VIEW green_source
       USING parquet
       OPTIONS (
         path 's3a://sirius-logs-riwi/tlc/raw/green/{{ var(\"anio\") }}/green_tripdata_{{ var(\"anio\") }}-{{ \"%02d\" | format(var(\"mes\") | int) }}.parquet'
       )"
    ],
    spark_conf={
      'spark.sql.shuffle.partitions': '11',
      'spark.sql.adaptive.enabled': 'true',
      'spark.sql.adaptive.coalescePartitions.enabled': 'true',
      'spark.sql.adaptive.coalescePartitions.minPartitionNum': '11',
      'spark.sql.files.maxPartitionBytes': '67108864',
      'spark.hadoop.fs.s3a.endpoint': 's3.us-east-2.amazonaws.com',
      'spark.hadoop.fs.s3a.endpoint.region': 'us-east-2',
      'spark.hadoop.fs.s3a.path.style.access': 'true'
    }
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
        {% if var("anio") | int == 2024 %}
            CAST(VendorID AS INTEGER) AS vendor_id,
            CAST(lpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime,
            CAST(lpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
            CAST(RatecodeID AS INTEGER) AS ratecode_id,
            CAST(PUlocationID AS INTEGER) AS pu_location_id,
            CAST(DOlocationID AS INTEGER) AS do_location_id,
            CAST(passenger_count AS INTEGER) AS passenger_count,
            CAST(trip_distance AS DOUBLE) AS trip_distance,
            CAST(fare_amount AS DOUBLE) AS fare_amount,
            CAST(extra AS DOUBLE) AS extra,
            CAST(mta_tax AS DOUBLE) AS mta_tax,
            CAST(tip_amount AS DOUBLE) AS tip_amt,
            CAST(tolls_amount AS DOUBLE) AS tolls_amt,
            CAST(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
            CAST(cbd_congestion_fee AS DOUBLE) AS cbd_congestion_fee,
            CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
            CAST(total_amount AS DOUBLE) AS total_amt,

            COALESCE(CAST(fare_amount AS DOUBLE),0) + 
            COALESCE(CAST(extra AS DOUBLE),0) +
            COALESCE(CAST(mta_tax AS DOUBLE),0) +
            COALESCE(CAST(tip_amount AS DOUBLE),0) +
            COALESCE(CAST(tolls_amount AS DOUBLE),0) +
            COALESCE(CAST(congestion_surcharge AS DOUBLE),0) +
            COALESCE(CAST(improvement_surcharge AS DOUBLE),0) AS true_total_amt,

            COALESCE(CAST(total_amount AS DOUBLE),0) -
            (COALESCE(CAST(fare_amount AS DOUBLE),0) + 
            COALESCE(CAST(extra AS DOUBLE),0) +
            COALESCE(CAST(mta_tax AS DOUBLE),0) +
            COALESCE(CAST(tip_amount AS DOUBLE),0) +
            COALESCE(CAST(tolls_amount AS DOUBLE),0) +
            COALESCE(CAST(congestion_surcharge AS DOUBLE),0) +
            COALESCE(CAST(improvement_surcharge AS DOUBLE),0)) AS comparation_total_amt,

            CAST(payment_type AS INTEGER) AS payment_type,
            CAST(trip_type AS INTEGER) AS trip_type

        {% else %}
            CAST(VendorID AS INTEGER) AS vendor_id,
            CAST(lpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime,
            CAST(lpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
            CAST(RatecodeID AS INTEGER) AS ratecode_id,
            CAST(PULocationID AS INTEGER) AS pu_location_id,
            CAST(DOLocationID AS INTEGER) AS do_location_id,
            CAST(passenger_count AS INTEGER) AS passenger_count,
            CAST(trip_distance AS DOUBLE) AS trip_distance,
            CAST(fare_amount AS DOUBLE) AS fare_amount,
            CAST(extra AS DOUBLE) AS extra,
            CAST(mta_tax AS DOUBLE) AS mta_tax,
            CAST(tip_amount AS DOUBLE) AS tip_amt,
            CAST(tolls_amount AS DOUBLE) AS tolls_amt,
            CAST(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
            CAST(cbd_congestion_fee AS DOUBLE) AS cbd_congestion_fee,
            CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
            CAST(total_amount AS DOUBLE) AS total_amt,

            COALESCE(CAST(fare_amount AS DOUBLE),0) + 
            COALESCE(CAST(extra AS DOUBLE),0) +
            COALESCE(CAST(mta_tax AS DOUBLE),0) +
            COALESCE(CAST(tip_amount AS DOUBLE),0) +
            COALESCE(CAST(tolls_amount AS DOUBLE),0) +
            COALESCE(CAST(congestion_surcharge AS DOUBLE),0) +
            COALESCE(CAST(improvement_surcharge AS DOUBLE),0) AS true_total_amt,

            COALESCE(CAST(total_amount AS DOUBLE),0) -
            (COALESCE(CAST(fare_amount AS DOUBLE),0) + 
            COALESCE(CAST(extra AS DOUBLE),0) +
            COALESCE(CAST(mta_tax AS DOUBLE),0) +
            COALESCE(CAST(tip_amount AS DOUBLE),0) +
            COALESCE(CAST(tolls_amount AS DOUBLE),0) +
            COALESCE(CAST(congestion_surcharge AS DOUBLE),0) +
            COALESCE(CAST(improvement_surcharge AS DOUBLE),0)) AS comparation_total_amt,

            CAST(payment_type AS INTEGER) AS payment_type,
            CAST(trip_type AS INTEGER) AS trip_type
        {% endif %}
    FROM raw_data
),

-- 3. Filter anomalies in dates 
correct_date AS (
    SELECT *
    FROM normalized_columns
    WHERE EXTRACT(year from tpep_pickup_datetime) = {{var("anio")}}
    AND EXTRACT(year from  tpep_dropoff_datetime) = {{var("anio")}}
),

-- 4. Delete Duplicated
duplicated_delete AS (
  SELECT *
  FROM (SELECT *,
        ROW_NUMBER() OVER (
          PARTITION BY
            vendor_id,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            pu_location_id,
            do_location_id
          ORDER BY tpep_pickup_datetime) AS rn
      FROM correct_date)
  WHERE rn = 1
),

-- 5. Delete Anomalies in trip distance and total amount
delete_amount_anomalies AS (
  SELECT *
  FROM duplicated_delete
  WHERE total_amt >=0 AND trip_distance>=0 
),

-- 6. Delete Date anomalies
delete_date_anomalies AS(
  SELECT *
  FROM delete_amount_anomalies
  WHERE tpep_dropoff_datetime>= tpep_pickup_datetime
),

-- 7. Nulls management 

-- 7.1 Firts null filter: ratecode_id and amount columns
nulls_first_filter AS (
    SELECT 
        vendor_id, 
        tpep_pickup_datetime, 
        tpep_dropoff_datetime,
        COALESCE(ratecode_id, 1) AS ratecode_id,
        pu_location_id, 
        do_location_id, 
        passenger_count,
        trip_distance, 
        COALESCE(fare_amount, 0) AS fare_amount, 
        COALESCE(extra, 0) AS extra, 
        COALESCE(mta_tax, 0) AS mta_tax, 
        COALESCE(tip_amt, 0) AS tip_amt, 
        COALESCE(tolls_amt, 0) AS tolls_amt,
        COALESCE(congestion_surcharge, 0) AS congestion_surcharge,
        COALESCE(cbd_congestion_fee, 0) AS cbd_congestion_fee, 
        COALESCE(improvement_surcharge, 0) AS improvement_surcharge,
        total_amt, 
        true_total_amt, 
        comparation_total_amt,
        payment_type,
        COALESCE(trip_type, 1) AS trip_type
    FROM delete_date_anomalies
),

-- 7.2 Second null filter: passenger_count fixing
stats_passenger AS (
  SELECT FLOOR(AVG(passenger_count)) AS passenger_count_avg
  FROM nulls_first_filter
  WHERE passenger_count is not null and passenger_count <= 6
),

nulls_second_filter AS(
  SELECT /*+ BROADCAST(s) */
    n.* EXCEPT(passenger_count),
    CASE 
      WHEN  n.passenger_count IS NULL THEN passenger_count_avg
      WHEN n.passenger_count > 6 THEN 6
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
        WHERE payment_type is not null
        GROUP BY payment_type
    )
),

nulls_third_filter AS(
  SELECT /*+ BROADCAST(r) */
    n.* EXCEPT (payment_type, rand_val),
    COALESCE(n.payment_type, r.payment_type) AS payment_type
  FROM(
    SELECT *, RAND() AS rand_val
    FROM nulls_second_filter
  ) n
  LEFT JOIN range_payment r
    ON n.payment_type IS NULL
    AND n.rand_val >= r.low_limit
    AND n.rand_val < r.upper_limit
),

--8. Outliers management: amount columns atypical Values
stats_percentiles AS (
  SELECT 
    PERCENTILE(trip_distance, 0.99) AS p99_trip_distance,
    PERCENTILE(fare_amount, 0.99) AS p99_fare_amount,
    PERCENTILE(extra, 0.99) AS p99_extra,
    PERCENTILE(mta_tax, 0.99) AS p99_mta_tax,
    PERCENTILE(tip_amt, 0.99) AS p99_tip_amt,
    PERCENTILE(tolls_amt, 0.99) AS p99_tolls_amt,
    PERCENTILE(congestion_surcharge, 0.99) AS p99_congestion_surcharge,
    PERCENTILE(cbd_congestion_fee, 0.99) AS p99_cbd_congestion_fee,
    PERCENTILE(improvement_surcharge, 0.99) AS p99_improvement_surcharge,
    PERCENTILE(total_amt, 0.99) AS p99_total_amt,
    PERCENTILE(true_total_amt,  0.99) AS p99_true_total_amt
  FROM nulls_third_filter
),

calculated_max AS (
  SELECT 
    p99_trip_distance * 1.5 AS max_trip_distance,
    p99_fare_amount * 1.5 AS max_fare_amount,
    p99_extra * 1.5 AS max_extra,
    p99_mta_tax * 1.5 AS max_mta_tax,
    p99_tip_amt * 1.5 AS max_tip_amt,
    p99_tolls_amt * 1.5 AS max_tolls_amt,
    p99_congestion_surcharge * 1.5 AS max_congestion_surcharge,
    p99_cbd_congestion_fee  * 1.5 AS max_cbd_congestion_fee,
    p99_improvement_surcharge  * 1.5 AS max_improvement_surcharge,
    p99_total_amt  * 1.5 AS max_total_amt,
    p99_true_total_amt * 1.5 AS max_true_total_amt
  FROM stats_percentiles
),

atypical_amount_values_fixing AS (
  SELECT /*+ BROADCAST(c) */
    n.* EXCEPT (trip_distance,fare_amount, extra,mta_tax,tip_amt,tolls_amt,congestion_surcharge,cbd_congestion_fee, improvement_surcharge,total_amt,true_total_amt),
    CASE
      WHEN n.trip_distance > c.max_trip_distance THEN c.max_trip_distance
      ELSE n.trip_distance
    END AS trip_distance,

    CASE
      WHEN n.fare_amount > c.max_fare_amount THEN c.max_fare_amount
      ELSE n.fare_amount
    END AS fare_amount,

    CASE
      WHEN n.extra > c.max_extra THEN c.max_extra
      ELSE n.extra
    END AS extra,

    CASE
      WHEN n.mta_tax > c.max_mta_tax THEN c.max_mta_tax
      ELSE n.mta_tax
    END AS mta_tax,

    CASE
      WHEN n.tip_amt > c.max_tip_amt THEN c.max_tip_amt
      ELSE n.tip_amt
    END AS tip_amt,

    CASE
      WHEN n.tolls_amt > c.max_tolls_amt THEN c.max_tolls_amt
      ELSE n.tolls_amt
    END AS tolls_amt,

    CASE
      WHEN n.congestion_surcharge > c.max_congestion_surcharge THEN c.max_congestion_surcharge
      ELSE n.congestion_surcharge
    END AS congestion_surcharge,
    
    CASE
      WHEN n.cbd_congestion_fee > c.max_cbd_congestion_fee THEN c.max_cbd_congestion_fee
      ELSE n.cbd_congestion_fee
    END AS cbd_congestion_fee,

    CASE
      WHEN n.improvement_surcharge > c.max_improvement_surcharge THEN c.max_improvement_surcharge
      ELSE n.improvement_surcharge
    END AS improvement_surcharge,

    CASE
      WHEN n.total_amt > c.max_total_amt THEN c.max_total_amt
      ELSE n.total_amt
    END AS total_amt,

    CASE
      WHEN n.true_total_amt > c.max_true_total_amt THEN c.max_true_total_amt
      ELSE n.true_total_amt
    END AS true_total_amt

  FROM nulls_third_filter n
  CROSS JOIN  calculated_max c
)

-- FInal select
SELECT *
FROM atypical_amount_values_fixing;