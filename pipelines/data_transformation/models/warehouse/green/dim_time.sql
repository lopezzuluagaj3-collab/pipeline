{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/green/dim_time',
    spark_conf={
      'spark.sql.shuffle.partitions': '11',
      'spark.sql.adaptive.enabled': 'true',
      'spark.sql.adaptive.coalescePartitions.enabled': 'true',
      'spark.hadoop.fs.s3a.endpoint': 's3.us-east-2.amazonaws.com',
      'spark.hadoop.fs.s3a.endpoint.region': 'us-east-2',
      'spark.hadoop.fs.s3a.path.style.access': 'true'
    }
  )
}}

WITH times AS (
    SELECT DISTINCT
        HOUR(tpep_pickup_datetime)   AS hour,
        MINUTE(tpep_pickup_datetime) AS minute
    FROM {{ source('staging', 'stg_green') }}
    WHERE tpep_pickup_datetime IS NOT NULL
),

dim_time AS (
    SELECT
        CAST(
            LPAD(CAST(hour AS STRING), 2, '0') ||
            LPAD(CAST(minute AS STRING), 2, '0')
        AS INTEGER)                  AS time_id,

        hour,
        minute,

        CASE
            WHEN hour BETWEEN 0  AND 5  THEN 'early morning'
            WHEN hour BETWEEN 6  AND 11 THEN 'morning'
            WHEN hour BETWEEN 12 AND 16 THEN 'afternoon'
            WHEN hour BETWEEN 17 AND 20 THEN 'evening'
            WHEN hour BETWEEN 21 AND 23 THEN 'night'
        END                          AS period_of_day

    FROM times
)

SELECT * FROM dim_time;