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
);

-- 2. Rename columns to a normalized name

normalized_columns AS (
    SELECT 
        {% if var("anio") | int = 2024%}
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
            CAST(cbd_congestion_fee AS DOUBLE) AS cbd_congestion_fee
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
)


-- 3. Filter anomalies in dates 
correct_date AS (
    SELECT *
    FROM normalized_columns
    WHERE EXTRACT(year from tpep_pickup_datetime) = {{var("anio")}}
    AND EXTRACT(year from  tpep_dropoff_datetime) = {{var("anio")}}
)

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
)

-- 5. Delete Anomalies in trip distance and total amount

delete_amount_anomalies AS (
  SELECT *
  FROM duplicated_delete
  WHERE total_amt >=0 AND trip_distance>=0
)

-- 6. Delete Date anomalies

delete_date_anomalies AS(
  SELECT *
  FROM delete_amount_anomalies
  WHERE tpep_dropoff_datetime>= tpep_pickup_datetime
)

-- 7. Nulls management ratecode_id and Amount Columns

nulls_first_filter AS (
    SELECT vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime,
        COALESCE(ratecode_id, 1) AS ratecode_id,
        pu_location_id, do_location_id, 

        CASE
          WHEN passenger_count IS NULL
          THEN CASE WHEN FLOOR(AVG(passenger_count))
          ELSE passenger_count
        END AS passenger_count, 

        trip_distance, fare_amount, extra, mta_tax, tip_amt, tolls_amt,
        COALESCE(congestion_surcharge, 0) AS congestion_surcharge,
        cbd_congestion_fee, improvement_surcharge,
        total_amt, true_total_amt, comparation_total_amt,
      
        CASE
          WHEN payment_type IS NULL
          THEN CASE WHEN (
            SELECT COUNT(payment_type) 
            FROM delete_date_anomalies
            GROUP BY payment_type
            ORDER BY COUNT(payment_type) DESC
            LIMIT 1)
          ELSE payment_type
        END AS payment_type, 
        
        CASE
          WHEN trip_type IS NULL
          THEN CASE WHEN RAND() <=  0.97 THEN 1 ELSE 2 END 
          ELSE trip_type
        END AS trip_type
       FROM delete_date_anomalies
)


-- FInal select
SELECT *
FROM limpio




SELECT columna, COUNT(*) AS frecuencia
FROM tabla
GROUP BY columna
ORDER BY frecuencia DESC
LIMIT 1;
