{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/green/dim_trip_type',
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

WITH dim_trip_type AS (
    SELECT *
    FROM (
        VALUES
            (1, 'Street-hail', 'Trip hailed directly on the street by the passenger'),
            (2, 'Dispatch',    'Trip dispatched by the LPEP provider')
    ) AS t(trip_type_id, trip_type_name, description)
)

SELECT * FROM dim_trip_type;