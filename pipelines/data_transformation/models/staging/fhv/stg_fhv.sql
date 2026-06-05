{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/staging/fhv/anio={{ var("anio") }}/mes={{ "%02d" | format(var("mes") | int) }}',
    pre_hook=[
      "CREATE OR REPLACE TEMPORARY VIEW fhv_source USING parquet OPTIONS (path 's3a://sirius-logs-riwi/tlc/raw/fhv/{{ var(\"anio\") }}/fhv_tripdata_{{ var(\"anio\") }}-{{ \"%02d\" | format(var(\"mes\") | int) }}.parquet')"
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
-- ─────────────────────────────────────────────
-- CTE 1: Leer el archivo parquet del mes/año exacto
-- ─────────────────────────────────────────────
WITH source AS (

    SELECT *
    FROM fhv_source

),

-- ─────────────────────────────────────────────
-- CTE 2: Renombrar columnas a snake_case final
-- Excluir: PUlocationID, DOlocationID, SR_Flag (alto % nulos — EDA FHV)
-- ─────────────────────────────────────────────
renamed AS (

    SELECT
        dispatching_base_num,
        Affiliated_base_number  AS affiliated_base_number,
        pickup_datetime         AS tpep_pickup_datetime,
        dropOff_datetime        AS tpep_dropoff_datetime
    FROM source

),

-- ─────────────────────────────────────────────
-- CTE 3: Filtrar solo registros del año correcto
-- El EDA detectó fechas de otros años infiltradas en los archivos
-- ─────────────────────────────────────────────
filtrado_anio AS (

    SELECT *
    FROM renamed
    WHERE YEAR(tpep_pickup_datetime) = {{ var("anio") }}

),

-- ─────────────────────────────────────────────
-- CTE 4: Eliminar duplicados con DISTINCT
-- Evita shuffle pesado de ROW_NUMBER() — previene OOM en workers de 1GB
-- ─────────────────────────────────────────────
sin_duplicados AS (

    SELECT DISTINCT
        dispatching_base_num,
        affiliated_base_number,
        tpep_pickup_datetime,
        tpep_dropoff_datetime
    FROM filtrado_anio

),

-- ─────────────────────────────────────────────
-- CTE 5: Eliminar filas donde affiliated_base_number es nulo o vacío
-- EDA FHV: cuando sea nulo → borrar toda la fila
-- ─────────────────────────────────────────────
sin_nulos_affiliated AS (

    SELECT *
    FROM sin_duplicados
    WHERE affiliated_base_number IS NOT NULL
      AND TRIM(affiliated_base_number) != ''

),

-- ─────────────────────────────────────────────
-- CTE 6: Eliminar registros con timestamps inválidos
-- EDA FHV: pickup >= dropoff → eliminar fila
-- ─────────────────────────────────────────────
tiempos_validos AS (

    SELECT *
    FROM sin_nulos_affiliated
    WHERE tpep_pickup_datetime < tpep_dropoff_datetime

),

-- ─────────────────────────────────────────────
-- CTE 7: Agregar columnas de partición para escritura en S3
-- ─────────────────────────────────────────────
final AS (

    SELECT
        dispatching_base_num,
        affiliated_base_number,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        CAST({{ var("anio") }} AS INTEGER) AS anio,
        CAST({{ var("mes") }}  AS INTEGER) AS mes
    FROM tiempos_validos

)

SELECT * FROM final
