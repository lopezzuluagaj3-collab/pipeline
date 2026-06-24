{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/green/dim_date',
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

WITH dates AS (
    SELECT DISTINCT
        CAST(DATE(tpep_pickup_datetime) AS DATE) AS full_date
    FROM {{ source('staging', 'stg_green') }}
    WHERE tpep_pickup_datetime IS NOT NULL
),

dim_date AS (
    SELECT
        CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INTEGER)  AS date_id,
        full_date,
        YEAR(full_date)                                       AS year,
        MONTH(full_date)                                      AS month,
        DATE_FORMAT(full_date, 'MMMM')                        AS month_name,
        DAY(full_date)                                        AS day,
        DAYOFWEEK(full_date)                                  AS day_of_week,
        DATE_FORMAT(full_date, 'EEEE')                        AS day_name,
        WEEKOFYEAR(full_date)                                 AS week_of_year,
        QUARTER(full_date)                                    AS quarter,
        CASE
            WHEN DAYOFWEEK(full_date) IN (1, 7) THEN TRUE
            ELSE FALSE
        END                                                   AS is_weekend
    FROM dates
)

SELECT * FROM dim_date;