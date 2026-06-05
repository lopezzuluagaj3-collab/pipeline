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
        {% if var("anio") | int == 2024%}
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


-- 2. Filter anomalies in dates 
correct_date AS (
    SELECT *
    FROM green_source
    WHERE pick_

)

SELECT nombre || apellido || cast(edad as varchar(2)) AS nombre_completo
FROM clientes;


-- FInal select
SELECT *
FROM limpio
