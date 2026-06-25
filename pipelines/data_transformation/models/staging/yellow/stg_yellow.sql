{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/staging/yellow/anio=' ~ var("anio") ~ '/mes=' ~ "%02d" | format(var("mes") | int),
    pre_hook=[
      "SET spark.sql.shuffle.partitions = 13",
      "SET spark.sql.adaptive.enabled = true",
      "SET spark.sql.adaptive.coalescePartitions.enabled = true",
      "SET spark.sql.adaptive.coalescePartitions.minPartitionNum = 13",
      "SET spark.sql.files.maxPartitionBytes = 67108864",

      "{% if var('anio') | int == 2009 %}
      CREATE OR REPLACE TEMPORARY VIEW yellow_source AS
      SELECT
        CASE
          WHEN LOWER(COALESCE(vendor_name, '')) IN ('creative mobile technologies', 'cmt') THEN 1
          WHEN LOWER(COALESCE(vendor_name, '')) IN ('verifone inc', 'verifone', 'vts')     THEN 2
          ELSE 1
        END                                          AS vendor_id_norm,
        CAST(Trip_Pickup_DateTime AS TIMESTAMP)      AS pickup_dt,
        CAST(Trip_Dropoff_DateTime AS TIMESTAMP)     AS dropoff_dt,
        CAST(Passenger_Count AS INTEGER)             AS passenger_count,
        CAST(Trip_Distance AS DOUBLE)                AS trip_distance,
        CAST(NULL AS INTEGER)                        AS PULocationID,
        CAST(NULL AS INTEGER)                        AS DOLocationID,
        CAST(NULL AS INTEGER)                        AS RatecodeID,
        CASE
          WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), '')) IN ('credit card', 'cre') THEN 1
          WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), '')) IN ('cash', 'csh')        THEN 2
          WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), '')) IN ('no charge', 'noc')   THEN 3
          WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), '')) IN ('dispute', 'dis')     THEN 4
          WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), '')) IN ('unknown', 'unk')     THEN 5
          WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), '')) IN ('voided trip', 'voi') THEN 6
          ELSE 1
        END                                          AS payment_type_norm,
        CAST(Fare_Amt AS DOUBLE)                     AS fare_amount,
        CAST(0.0 AS DOUBLE)                          AS extra,
        CAST(Tip_Amt AS DOUBLE)                      AS tip_amt,
        CAST(Tolls_Amt AS DOUBLE)                    AS tolls_amt,
        CAST(Total_Amt AS DOUBLE)                    AS total_amt,
        CAST(COALESCE(surcharge, 0.0) AS DOUBLE)     AS surcharge,
        CAST(COALESCE(mta_tax, 0.0) AS DOUBLE)       AS mta_tax,
        CAST(0.0 AS DOUBLE)                          AS congestion_surcharge,
        CAST(0.0 AS DOUBLE)                          AS airport_fee,
        CAST(0.0 AS DOUBLE)                          AS cbd_congestion_fee,
        CAST(Start_Lon AS DOUBLE)                    AS start_lon,
        CAST(Start_Lat AS DOUBLE)                    AS start_lat,
        CAST(End_Lon AS DOUBLE)                      AS end_lon,
        CAST(End_Lat AS DOUBLE)                      AS end_lat
      FROM parquet.`s3a://sirius-logs-riwi/tlc/raw/yellow/{{ var('anio') }}/yellow_tripdata_{{ var('anio') }}-{{ '%02d' | format(var('mes') | int) }}.parquet`

      {% elif var('anio') | int == 2010 %}
      CREATE OR REPLACE TEMPORARY VIEW yellow_source AS
      SELECT
        CASE
          WHEN LOWER(COALESCE(CAST(vendor_id AS STRING), '')) IN ('cmт', 'cmt', '1') THEN 1
          WHEN LOWER(COALESCE(CAST(vendor_id AS STRING), '')) IN ('vts', '2')         THEN 2
          ELSE 1
        END                                          AS vendor_id_norm,
        CAST(pickup_datetime AS TIMESTAMP)           AS pickup_dt,
        CAST(dropoff_datetime AS TIMESTAMP)          AS dropoff_dt,
        CAST(passenger_count AS INTEGER)             AS passenger_count,
        CAST(trip_distance AS DOUBLE)                AS trip_distance,
        CAST(NULL AS INTEGER)                        AS PULocationID,
        CAST(NULL AS INTEGER)                        AS DOLocationID,
        CAST(COALESCE(rate_code, NULL) AS INTEGER)   AS RatecodeID,
        CASE
          WHEN LOWER(COALESCE(CAST(payment_type AS STRING), '')) IN ('credit card', 'cre') THEN 1
          WHEN LOWER(COALESCE(CAST(payment_type AS STRING), '')) IN ('cash', 'csh')        THEN 2
          WHEN LOWER(COALESCE(CAST(payment_type AS STRING), '')) IN ('no charge', 'noc')   THEN 3
          WHEN LOWER(COALESCE(CAST(payment_type AS STRING), '')) IN ('dispute', 'dis')     THEN 4
          WHEN LOWER(COALESCE(CAST(payment_type AS STRING), '')) IN ('unknown', 'unk')     THEN 5
          WHEN LOWER(COALESCE(CAST(payment_type AS STRING), '')) IN ('voided trip', 'voi') THEN 6
          ELSE 1
        END                                          AS payment_type_norm,
        CAST(fare_amount AS DOUBLE)                  AS fare_amount,
        CAST(0.0 AS DOUBLE)                          AS extra,
        CAST(tip_amount AS DOUBLE)                   AS tip_amt,
        CAST(tolls_amount AS DOUBLE)                 AS tolls_amt,
        CAST(total_amount AS DOUBLE)                 AS total_amt,
        CAST(COALESCE(surcharge, 0.0) AS DOUBLE)     AS surcharge,
        CAST(COALESCE(mta_tax, 0.0) AS DOUBLE)       AS mta_tax,
        CAST(0.0 AS DOUBLE)                          AS congestion_surcharge,
        CAST(0.0 AS DOUBLE)                          AS airport_fee,
        CAST(0.0 AS DOUBLE)                          AS cbd_congestion_fee,
        CAST(pickup_longitude AS DOUBLE)             AS start_lon,
        CAST(pickup_latitude AS DOUBLE)              AS start_lat,
        CAST(dropoff_longitude AS DOUBLE)            AS end_lon,
        CAST(dropoff_latitude AS DOUBLE)             AS end_lat
      FROM parquet.`s3a://sirius-logs-riwi/tlc/raw/yellow/{{ var('anio') }}/yellow_tripdata_{{ var('anio') }}-{{ '%02d' | format(var('mes') | int) }}.parquet`

      {% elif var('anio') | int <= 2023 %}
      CREATE OR REPLACE TEMPORARY VIEW yellow_source AS
      SELECT
        CAST(VendorID AS INTEGER)                      AS vendor_id_norm,
        CAST(tpep_pickup_datetime AS TIMESTAMP)        AS pickup_dt,
        CAST(tpep_dropoff_datetime AS TIMESTAMP)       AS dropoff_dt,
        CAST(passenger_count AS INTEGER)               AS passenger_count,
        CAST(trip_distance AS DOUBLE)                  AS trip_distance,
        CAST(PULocationID AS INTEGER)                  AS PULocationID,
        CAST(DOLocationID AS INTEGER)                  AS DOLocationID,
        CAST(RatecodeID AS INTEGER)                    AS RatecodeID,
        CAST(payment_type AS INTEGER)                  AS payment_type_norm,
        CAST(fare_amount AS DOUBLE)                    AS fare_amount,
        CAST(COALESCE(extra, 0.0) AS DOUBLE)           AS extra,
        CAST(tip_amount AS DOUBLE)                     AS tip_amt,
        CAST(tolls_amount AS DOUBLE)                   AS tolls_amt,
        CAST(total_amount AS DOUBLE)                   AS total_amt,
        CAST(COALESCE(improvement_surcharge, 0.0) AS DOUBLE) AS surcharge,
        CAST(COALESCE(mta_tax, 0.0) AS DOUBLE)         AS mta_tax,
        CAST(COALESCE(congestion_surcharge, 0.0) AS DOUBLE) AS congestion_surcharge,
        CAST(COALESCE(airport_fee, 0.0) AS DOUBLE)     AS airport_fee,
        CAST(0.0 AS DOUBLE)                            AS cbd_congestion_fee,
        CAST(NULL AS DOUBLE)                           AS start_lon,
        CAST(NULL AS DOUBLE)                           AS start_lat,
        CAST(NULL AS DOUBLE)                           AS end_lon,
        CAST(NULL AS DOUBLE)                           AS end_lat
      FROM parquet.`s3a://sirius-logs-riwi/tlc/raw/yellow/{{ var('anio') }}/yellow_tripdata_{{ var('anio') }}-{{ '%02d' | format(var('mes') | int) }}.parquet`

      {% elif var('anio') | int == 2024 %}
      CREATE OR REPLACE TEMPORARY VIEW yellow_source AS
      SELECT
        CAST(VendorID AS INTEGER)                      AS vendor_id_norm,
        CAST(tpep_pickup_datetime AS TIMESTAMP)        AS pickup_dt,
        CAST(tpep_dropoff_datetime AS TIMESTAMP)       AS dropoff_dt,
        CAST(passenger_count AS INTEGER)               AS passenger_count,
        CAST(trip_distance AS DOUBLE)                  AS trip_distance,
        CAST(PULocationID AS INTEGER)                  AS PULocationID,
        CAST(DOLocationID AS INTEGER)                  AS DOLocationID,
        CAST(RatecodeID AS INTEGER)                    AS RatecodeID,
        CAST(payment_type AS INTEGER)                  AS payment_type_norm,
        CAST(fare_amount AS DOUBLE)                    AS fare_amount,
        CAST(COALESCE(extra, 0.0) AS DOUBLE)           AS extra,
        CAST(tip_amount AS DOUBLE)                     AS tip_amt,
        CAST(tolls_amount AS DOUBLE)                   AS tolls_amt,
        CAST(total_amount AS DOUBLE)                   AS total_amt,
        CAST(COALESCE(improvement_surcharge, 0.0) AS DOUBLE) AS surcharge,
        CAST(COALESCE(mta_tax, 0.0) AS DOUBLE)         AS mta_tax,
        CAST(COALESCE(congestion_surcharge, 0.0) AS DOUBLE) AS congestion_surcharge,
        CAST(COALESCE(Airport_fee, 0.0) AS DOUBLE)     AS airport_fee,
        CAST(0.0 AS DOUBLE)                            AS cbd_congestion_fee,
        CAST(NULL AS DOUBLE)                           AS start_lon,
        CAST(NULL AS DOUBLE)                           AS start_lat,
        CAST(NULL AS DOUBLE)                           AS end_lon,
        CAST(NULL AS DOUBLE)                           AS end_lat
      FROM parquet.`s3a://sirius-logs-riwi/tlc/raw/yellow/{{ var('anio') }}/yellow_tripdata_{{ var('anio') }}-{{ '%02d' | format(var('mes') | int) }}.parquet`

      {% else %}
      CREATE OR REPLACE TEMPORARY VIEW yellow_source AS
      SELECT
        CAST(VendorID AS INTEGER)                      AS vendor_id_norm,
        CAST(tpep_pickup_datetime AS TIMESTAMP)        AS pickup_dt,
        CAST(tpep_dropoff_datetime AS TIMESTAMP)       AS dropoff_dt,
        CAST(passenger_count AS INTEGER)               AS passenger_count,
        CAST(trip_distance AS DOUBLE)                  AS trip_distance,
        CAST(PULocationID AS INTEGER)                  AS PULocationID,
        CAST(DOLocationID AS INTEGER)                  AS DOLocationID,
        CAST(RatecodeID AS INTEGER)                    AS RatecodeID,
        CAST(payment_type AS INTEGER)                  AS payment_type_norm,
        CAST(fare_amount AS DOUBLE)                    AS fare_amount,
        CAST(COALESCE(extra, 0.0) AS DOUBLE)           AS extra,
        CAST(tip_amount AS DOUBLE)                     AS tip_amt,
        CAST(tolls_amount AS DOUBLE)                   AS tolls_amt,
        CAST(total_amount AS DOUBLE)                   AS total_amt,
        CAST(COALESCE(improvement_surcharge, 0.0) AS DOUBLE) AS surcharge,
        CAST(COALESCE(mta_tax, 0.0) AS DOUBLE)         AS mta_tax,
        CAST(COALESCE(congestion_surcharge, 0.0) AS DOUBLE) AS congestion_surcharge,
        CAST(COALESCE(Airport_fee, 0.0) AS DOUBLE)     AS airport_fee,
        CAST(cbd_congestion_fee AS DOUBLE)             AS cbd_congestion_fee,
        CAST(NULL AS DOUBLE)                           AS start_lon,
        CAST(NULL AS DOUBLE)                           AS start_lat,
        CAST(NULL AS DOUBLE)                           AS end_lon,
        CAST(NULL AS DOUBLE)                           AS end_lat
      FROM parquet.`s3a://sirius-logs-riwi/tlc/raw/yellow/{{ var('anio') }}/yellow_tripdata_{{ var('anio') }}-{{ '%02d' | format(var('mes') | int) }}.parquet`
      {% endif %}"
    ]
  )
}}

-- ─────────────────────────────────────────────
-- CTE 1: Leer desde la vista temporal yellow_source
-- ─────────────────────────────────────────────
WITH source AS (
    SELECT * FROM yellow_source
),

-- ─────────────────────────────────────────────
-- CTE 2: Renombrar columnas normalizadas
-- ─────────────────────────────────────────────
renamed AS (
    SELECT
        vendor_id_norm                               AS vendor_id,
        pickup_dt                                    AS tpep_pickup_datetime,
        dropoff_dt                                   AS tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        PULocationID                                 AS pu_location_id_raw,
        DOLocationID                                 AS do_location_id_raw,
        RatecodeID                                   AS ratecode_id_raw,
        payment_type_norm                            AS payment_type_raw,
        fare_amount,
        extra,
        tip_amt,
        tolls_amt,
        total_amt,
        surcharge,
        mta_tax,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        start_lon,
        start_lat,
        end_lon,
        end_lat
    FROM source
),

-- ─────────────────────────────────────────────
-- CTE 3: Filtrar fechas atemporales
-- ─────────────────────────────────────────────
fechas_validas AS (
    SELECT *
    FROM renamed
    WHERE YEAR(tpep_pickup_datetime) = {{ var("anio") }}
      AND tpep_pickup_datetime < tpep_dropoff_datetime
),

-- ─────────────────────────────────────────────
-- CTE 4: Eliminar duplicados
-- ─────────────────────────────────────────────
sin_duplicados AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY
                       vendor_id,
                       tpep_pickup_datetime,
                       tpep_dropoff_datetime,
                       pu_location_id_raw,
                       do_location_id_raw
                   ORDER BY tpep_pickup_datetime
               ) AS rn
        FROM fechas_validas
    )
    WHERE rn = 1
),

-- ─────────────────────────────────────────────
-- CTE 5: Normalizar ratecode_id y payment_type
-- ─────────────────────────────────────────────
ratecode_normalizado AS (
    SELECT
        *,
        CASE
            WHEN ratecode_id_raw IS NULL             THEN 1
            WHEN ratecode_id_raw NOT BETWEEN 1 AND 6 THEN 1
            ELSE ratecode_id_raw
        END AS ratecode_id,
        CASE
            WHEN payment_type_raw IS NULL             THEN 1
            WHEN payment_type_raw NOT BETWEEN 1 AND 6 THEN 1
            ELSE payment_type_raw
        END AS payment_type
    FROM sin_duplicados
),

-- ─────────────────────────────────────────────
-- CTE 6: Manejar nulos en passenger_count
-- ─────────────────────────────────────────────
passenger_fixed AS (
    SELECT
        *,
        CASE
            WHEN passenger_count IS NULL OR passenger_count <= 0
            THEN CAST(FLOOR(AVG(passenger_count) OVER ()) AS INTEGER)
            ELSE passenger_count
        END AS passenger_count_clean
    FROM ratecode_normalizado
),

-- ─────────────────────────────────────────────
-- CTE 7: Manejar nulos en pu_location_id y do_location_id
-- 2009-2010: coordenadas GPS → zonas aleatorias top 2011
-- 2011+: nulos → 264
-- ─────────────────────────────────────────────
locations_fixed AS (
    SELECT
        *,
        CASE
            WHEN pu_location_id_raw IS NULL AND {{ var("anio") }} <= 2010
            THEN CAST((ARRAY(161, 162, 230, 236, 237))[CAST(RAND() * 5 AS INT)] AS INTEGER)
            WHEN pu_location_id_raw IS NULL
            THEN 264
            ELSE pu_location_id_raw
        END AS pu_location_id,
        CASE
            WHEN do_location_id_raw IS NULL AND {{ var("anio") }} <= 2010
            THEN CAST((ARRAY(161, 162, 230, 236, 237))[CAST(RAND() * 5 AS INT)] AS INTEGER)
            WHEN do_location_id_raw IS NULL
            THEN 264
            ELSE do_location_id_raw
        END AS do_location_id
    FROM passenger_fixed
),

-- ─────────────────────────────────────────────
-- CTE 8: Calcular true_total_amt
-- ─────────────────────────────────────────────
totales AS (
    SELECT
        *,
        ROUND(
            fare_amount + extra + tip_amt + tolls_amt +
            surcharge + mta_tax + congestion_surcharge +
            airport_fee + cbd_congestion_fee,
        2) AS true_total_amt
    FROM locations_fixed
),

-- ─────────────────────────────────────────────
-- CTE 9: Tabla final
-- ─────────────────────────────────────────────
final AS (
    SELECT
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count_clean                          AS passenger_count,
        trip_distance,
        pu_location_id,
        do_location_id,
        ratecode_id,
        payment_type,
        fare_amount,
        extra,
        tip_amt,
        tolls_amt,
        total_amt,
        true_total_amt,
        (ABS(total_amt - true_total_amt) < 0.01)      AS comparation_total_amt,
        surcharge,
        mta_tax,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        CAST({{ var("anio") }} AS INTEGER)             AS anio,
        CAST({{ var("mes") }}  AS INTEGER)             AS mes
    FROM totales
)

SELECT * FROM final  
