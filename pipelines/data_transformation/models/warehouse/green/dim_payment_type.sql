{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/green/dim_payment_type',
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

WITH dim_payment_type AS (
    SELECT *
    FROM (
        VALUES
            (0, 'Flex Fare',    'Dynamic pricing trip'),
            (1, 'Credit card',  'Payment by credit card'),
            (2, 'Cash',         'Payment in cash'),
            (3, 'No charge',    'Trip with no charge applied'),
            (4, 'Dispute',      'Payment in dispute'),
            (5, 'Unknown',      'Payment method unknown'),
            (6, 'Voided trip',  'Trip was voided')
    ) AS t(payment_type_id, payment_type_name, description)
)

SELECT * FROM dim_payment_type;