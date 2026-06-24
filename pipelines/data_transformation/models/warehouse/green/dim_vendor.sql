{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/green/dim_vendor',
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

WITH dim_vendor AS (
    SELECT *
    FROM (
        VALUES
            (1, 'Creative Mobile Technologies LLC', 'LPEP provider'),
            (2, 'Curb Mobility LLC',                'LPEP provider'),
            (6, 'Myle Technologies Inc',            'LPEP provider')
    ) AS t(vendor_id, company_name, description)
)

SELECT * FROM dim_vendor;