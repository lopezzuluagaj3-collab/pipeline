{{ config(
    materialized = 'table',
    schema       = 'fhv_intermediate'
) }}

/*
  Tabla de hechos de viajes FHV.

  Granularidad: 1 fila = 1 viaje FHV.
  Medidas:      trip_duration_minutes (calculada), pickup_hour, dropoff_hour.
  Claves FK:    base_key, affiliated_base_key, pickup_date_key, dropoff_date_key.

  Nota: surrogate trip_id no es estable entre ejecuciones. Si se requiere
  idempotencia, reemplazar por MD5(dispatching_base_num || pickup_datetime || ...).
*/

WITH source AS (

    SELECT
        dispatching_base_num,
        affiliated_base_number,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        anio,
        mes
    FROM {{ source('staging', 'fhv') }}
    WHERE dispatching_base_num IS NOT NULL
      AND tpep_pickup_datetime  IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL

),

dim_base AS (

    SELECT base_key, dispatching_base_num
    FROM {{ ref('dim_fhv_base') }}

),

dim_affiliated AS (

    SELECT affiliated_base_key, affiliated_base_number
    FROM {{ ref('dim_fhv_affiliated_base') }}

),

enriquecido AS (

    SELECT
        s.tpep_pickup_datetime,
        s.tpep_dropoff_datetime,
        s.anio,
        s.mes,

        b.base_key,
        a.affiliated_base_key,

        TO_CHAR(DATE(s.tpep_pickup_datetime),  'YYYYMMDD')::INTEGER AS pickup_date_key,
        TO_CHAR(DATE(s.tpep_dropoff_datetime), 'YYYYMMDD')::INTEGER AS dropoff_date_key,

        ROUND(
            EXTRACT(EPOCH FROM (s.tpep_dropoff_datetime - s.tpep_pickup_datetime)) / 60.0,
            2
        )                                                           AS trip_duration_minutes,

        EXTRACT(HOUR FROM s.tpep_pickup_datetime)::INTEGER          AS pickup_hour,
        EXTRACT(HOUR FROM s.tpep_dropoff_datetime)::INTEGER         AS dropoff_hour

    FROM source s
    LEFT JOIN dim_base b
           ON UPPER(TRIM(s.dispatching_base_num))  = b.dispatching_base_num
    LEFT JOIN dim_affiliated a
           ON UPPER(TRIM(s.affiliated_base_number)) = a.affiliated_base_number

),

final AS (

    SELECT
        ROW_NUMBER() OVER (
            ORDER BY pickup_date_key, base_key, tpep_pickup_datetime
        )                        AS trip_id,

        -- Claves foráneas
        base_key,
        affiliated_base_key,
        pickup_date_key,
        dropoff_date_key,

        -- Timestamps completos (útil para análisis intra-día)
        tpep_pickup_datetime     AS pickup_datetime,
        tpep_dropoff_datetime    AS dropoff_datetime,

        -- Medidas
        trip_duration_minutes,
        pickup_hour,
        dropoff_hour,

        -- Partición
        anio,
        mes

    FROM enriquecido

)

SELECT * FROM final
