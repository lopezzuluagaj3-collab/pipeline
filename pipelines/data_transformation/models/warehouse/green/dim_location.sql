{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/green/dim_location',
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

WITH raw_zones AS (
    SELECT *
    FROM {{ source('raw', 'taxi_zone_lookup') }}
),

dim_location AS (
    SELECT
        CAST(LocationID  AS INTEGER) AS location_id,
        CAST(Borough     AS STRING)  AS borough,
        CAST(Zone        AS STRING)  AS zone,
        CAST(service_zone AS STRING) AS service_zone
    FROM raw_zones
)

SELECT * FROM dim_location;