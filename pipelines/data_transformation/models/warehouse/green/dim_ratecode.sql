{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/green/dim_ratecode',
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

WITH dim_ratecode AS (
    SELECT *
    FROM (
        VALUES
            (1,  'Standard rate',          'Regular metered fare'),
            (2,  'JFK',                    'Flat rate to/from JFK airport'),
            (3,  'Newark',                 'Negotiated fare to/from Newark airport'),
            (4,  'Nassau or Westchester',  'Out of city fare'),
            (5,  'Negotiated fare',        'Pre-negotiated fare between driver and passenger'),
            (6,  'Group ride',             'Shared ride fare'),
            (99, 'Null/unknown',           'Rate code not recorded or unknown')
    ) AS t(ratecode_id, ratecode_name, description)
)

SELECT * FROM dim_ratecode;