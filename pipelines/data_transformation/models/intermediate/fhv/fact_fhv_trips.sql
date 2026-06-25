{{
  config(
    materialized = 'table',
    schema       = 'fhv_intermediate',
    tags         = ['fhv', 'intermediate', 'fact']
  )
}}

/*
  Tabla de hechos: Viajes FHV (For-Hire Vehicle)
  ------------------------------------------------
  FHV es el más simple de los 4 vehículos — no tiene montos
  ni zonas (fueron excluidas en el staging por alto % de nulos).

  Granularidad: 1 fila = 1 viaje.

  FKs a dimensiones:
    - pickup_datetime_id    → dim_fhv_datetime.datetime_id
    - dropoff_datetime_id   → dim_fhv_datetime.datetime_id
    - dispatching_base_num  → dim_fhv_base.base_num
    - affiliated_base_number → dim_fhv_base.base_num

  Medida principal derivada:
    - trip_duration_min
*/

SELECT
    -- Surrogate key
    MD5(
        COALESCE(dispatching_base_num,    '') || '|' ||
        COALESCE(affiliated_base_number,  '') || '|' ||
        COALESCE(tpep_pickup_datetime::TEXT,  '') || '|' ||
        COALESCE(tpep_dropoff_datetime::TEXT, '')
    )                                                           AS trip_id,

    -- FKs → Dimensiones
    DATE_TRUNC('hour', tpep_pickup_datetime)                    AS pickup_datetime_id,
    DATE_TRUNC('hour', tpep_dropoff_datetime)                   AS dropoff_datetime_id,
    dispatching_base_num,
    affiliated_base_number,

    -- Timestamps exactos
    tpep_pickup_datetime                                        AS pickup_datetime,
    tpep_dropoff_datetime                                       AS dropoff_datetime,

    -- Medida derivada: duración en minutos
    EXTRACT(
        EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)
    ) / 60.0                                                    AS trip_duration_min,

    -- Partición
    anio,
    mes

FROM {{ source('raw', 'formato_1') }}
