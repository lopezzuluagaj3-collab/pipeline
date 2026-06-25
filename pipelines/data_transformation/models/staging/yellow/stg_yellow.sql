{{
  config(
    materialized='table',
    file_format='parquet',
    location='s3a://sirius-logs-riwi/tlc/staging/yellow',
    partition_by=['anio', 'mes'],
    spark_conf={
      'spark.sql.shuffle.partitions': '13',
      'spark.sql.adaptive.enabled': 'true',
      'spark.sql.adaptive.coalescePartitions.enabled': 'true',
      'spark.sql.adaptive.coalescePartitions.minPartitionNum': '13',
      'spark.sql.files.maxPartitionBytes': '67108864'
    }
  )
}}

-- ─────────────────────────────────────────────
-- CTE 1: Leer el archivo parquet del mes/año exacto
-- ─────────────────────────────────────────────
WITH source AS (

    SELECT *
    FROM parquet.`s3a://sirius-logs-riwi/tlc/raw/yellow/{{ var("anio") }}/yellow_tripdata_{{ var("anio") }}-{{ "%02d" | format(var("mes") | int) }}.parquet`

),

-- ─────────────────────────────────────────────
-- CTE 2: Eliminar columna __index_level_0__ si existe
-- y renombrar todas las columnas a snake_case final
-- según el año del archivo
-- ─────────────────────────────────────────────
renamed AS (

    SELECT
        -- vendor_id: en 2009 viene como vendor_name (texto), se convierte a INTEGER
        CASE
            WHEN {{ var("anio") }} = 2009 THEN
                CASE
                    WHEN LOWER(COALESCE(vendor_name, '')) IN ('creative mobile technologies', 'cmt') THEN 1
                    WHEN LOWER(COALESCE(vendor_name, '')) IN ('verifone inc', 'verifone', 'vts')      THEN 2
                    ELSE 1
                END
            ELSE CAST(COALESCE(VendorID, vendor_id) AS INTEGER)
        END AS vendor_id,

        -- tpep_pickup_datetime
        CAST(
            COALESCE(
                tpep_pickup_datetime,
                pickup_datetime,
                Trip_Pickup_DateTime
            ) AS TIMESTAMP
        ) AS tpep_pickup_datetime,

        -- tpep_dropoff_datetime
        CAST(
            COALESCE(
                tpep_dropoff_datetime,
                dropoff_datetime,
                Trip_Dropoff_DateTime
            ) AS TIMESTAMP
        ) AS tpep_dropoff_datetime,

        -- passenger_count
        CAST(
            COALESCE(
                passenger_count,
                Passenger_Count
            ) AS INTEGER
        ) AS passenger_count,

        -- trip_distance
        CAST(
            COALESCE(
                trip_distance,
                Trip_Distance
            ) AS DOUBLE
        ) AS trip_distance,

        -- pu_location_id: en 2009-2010 venía como coordenadas GPS
        CAST(
            COALESCE(
                PULocationID,
                pu_location_id
            ) AS INTEGER
        ) AS pu_location_id_raw,

        -- do_location_id
        CAST(
            COALESCE(
                DOLocationID,
                do_location_id
            ) AS INTEGER
        ) AS do_location_id_raw,

        -- ratecode_id: en 2009 es 100% nulo, en otros años puede traer 99
        CAST(
            COALESCE(
                RatecodeID,
                rate_code,
                Rate_Code
            ) AS INTEGER
        ) AS ratecode_id_raw,

        -- payment_type: en 2009 viene como texto (cash/credit), se normaliza a INTEGER
        CASE
            WHEN {{ var("anio") }} = 2009 THEN
                CASE
                    WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), CAST(payment_type AS STRING), '')) IN ('credit card', 'cre') THEN 1
                    WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), CAST(payment_type AS STRING), '')) IN ('cash', 'csh')         THEN 2
                    WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), CAST(payment_type AS STRING), '')) IN ('no charge', 'noc')    THEN 3
                    WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), CAST(payment_type AS STRING), '')) IN ('dispute', 'dis')      THEN 4
                    WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), CAST(payment_type AS STRING), '')) IN ('unknown', 'unk')      THEN 5
                    WHEN LOWER(COALESCE(CAST(Payment_Type AS STRING), CAST(payment_type AS STRING), '')) IN ('voided trip', 'voi')  THEN 6
                    ELSE 1
                END
            ELSE CAST(COALESCE(payment_type, Payment_Type) AS INTEGER)
        END AS payment_type_raw,

        -- fare_amount
        CAST(COALESCE(fare_amount, Fare_Amt) AS DOUBLE) AS fare_amount,

        -- extra (cobros adicionales — columna nueva, puede no existir en años viejos)
        CAST(COALESCE(extra, 0.0) AS DOUBLE) AS extra,

        -- tip_amt
        CAST(COALESCE(tip_amount, Tip_Amt, tip_amt) AS DOUBLE) AS tip_amt,

        -- tolls_amt
        CAST(COALESCE(tolls_amount, Tolls_Amt, tolls_amt) AS DOUBLE) AS tolls_amt,

        -- total_amt
        CAST(COALESCE(total_amount, Total_Amt, total_amt) AS DOUBLE) AS total_amt,

        -- surcharge: en 2009 es 100% nulo porque el impuesto no existía → 0
        CAST(COALESCE(improvement_surcharge, Surcharge, surcharge, 0.0) AS DOUBLE) AS surcharge,

        -- mta_tax: en 2009 es 100% nulo → 0
        CAST(COALESCE(mta_tax, 0.0) AS DOUBLE) AS mta_tax,

        -- congestion_surcharge: nulo en 2009-2013 → 0
        CAST(COALESCE(congestion_surcharge, 0.0) AS DOUBLE) AS congestion_surcharge,

        -- airport_fee: nulo en 2009-2023 → 0
        CAST(COALESCE(airport_fee, Airport_fee, 0.0) AS DOUBLE) AS airport_fee,

        -- cbd_congestion_fee: solo existe desde 2024 → 0 en años anteriores
        CAST(COALESCE(cbd_congestion_fee, 0.0) AS DOUBLE) AS cbd_congestion_fee

    FROM source
    -- Eliminar columna __index_level_0__ (no se selecciona, simplemente no se incluye)

),

-- ─────────────────────────────────────────────
-- CTE 3: Filtrar fechas atemporales
-- Solo registros cuyo año de pickup coincide con el año del archivo
-- y donde pickup < dropoff (viaje lógicamente válido)
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

    SELECT DISTINCT *
    FROM fechas_validas

),

-- ─────────────────────────────────────────────
-- CTE 5: Normalizar ratecode_id
-- Valores fuera de rango 1-6 → 1 (estándar)
-- Nulos → 1
-- ─────────────────────────────────────────────
ratecode_normalizado AS (

    SELECT
        *,
        CASE
            WHEN ratecode_id_raw IS NULL              THEN 1
            WHEN ratecode_id_raw NOT BETWEEN 1 AND 6  THEN 1
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
-- Reemplazar nulos con el promedio redondeado hacia abajo (FLOOR)
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
-- 2009-2010: tenían coordenadas GPS, se asignan zonas aleatorias
--            basadas en las más concurridas del año 2011
--            (zonas top 2011: 237, 236, 161, 162, 230)
-- 2015+: nulos → 264 (zona no definida oficial)
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
-- CTE 8: Calcular true_total_amt y comparation_total_amt
-- true_total_amt = suma real de todos los componentes del viaje
-- comparation_total_amt = TRUE si total_amt coincide con true_total_amt
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
-- CTE 9: Ensamblar tabla final con columnas en orden definitivo
-- y agregar columnas de partición anio/mes
-- ─────────────────────────────────────────────
final AS (

    SELECT
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count_clean              AS passenger_count,
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
        (ABS(total_amt - true_total_amt) < 0.01) AS comparation_total_amt,
        surcharge,
        mta_tax,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        CAST({{ var("anio") }} AS INTEGER)  AS anio,
        CAST({{ var("mes") }}  AS INTEGER)  AS mes
    FROM totales

)

SELECT * FROM final
