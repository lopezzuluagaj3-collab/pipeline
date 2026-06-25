{{
  config(
    materialized = 'table',
    schema       = 'hvfhs_intermediate',
    tags         = ['hvfhs', 'intermediate', 'fact']
  )
}}

/*
  Tabla de hechos: Viajes HVFHS (Uber, Lyft, Via, Juno)
  -------------------------------------------------------
  HVFHS es el más completo de los 4 tipos de vehículo —
  tiene montos, zonas, timestamps de request/on_scene, y flags de servicio.

  Granularidad: 1 fila = 1 viaje.

  FKs a dimensiones:
    - request_datetime_id    → dim_hvfhs_datetime.datetime_id
    - pickup_datetime_id     → dim_hvfhs_datetime.datetime_id
    - dropoff_datetime_id    → dim_hvfhs_datetime.datetime_id
    - hvfhs_license_num      → dim_hvfhs_license.hvfhs_license_num
    - pu_location_id         → dim_hvfhs_location.location_id
    - do_location_id         → dim_hvfhs_location.location_id
*/

SELECT
    -- Surrogate key
    MD5(
        COALESCE(hvfhs_license_num,           '') || '|' ||
        COALESCE(dispatching_base_num,        '') || '|' ||
        COALESCE(tpep_pickup_datetime::TEXT,  '') || '|' ||
        COALESCE(tpep_dropoff_datetime::TEXT, '') || '|' ||
        COALESCE(pu_location_id::TEXT,        '') || '|' ||
        COALESCE(do_location_id::TEXT,        '')
    )                                                               AS trip_id,

    -- FKs → Dimensiones
    DATE_TRUNC('hour', request_datetime)                            AS request_datetime_id,
    DATE_TRUNC('hour', tpep_pickup_datetime)                        AS pickup_datetime_id,
    DATE_TRUNC('hour', tpep_dropoff_datetime)                       AS dropoff_datetime_id,
    hvfhs_license_num,
    dispatching_base_num,
    pu_location_id,
    do_location_id,

    -- Timestamps exactos
    request_datetime,
    on_scene_datetime,
    tpep_pickup_datetime                                            AS pickup_datetime,
    tpep_dropoff_datetime                                           AS dropoff_datetime,

    -- Medidas de distancia y tiempo
    trip_distance,
    trip_time                                                       AS trip_time_seconds,

    -- Medidas de ingresos (desglose completo)
    fare_amount,
    tolls_amt,
    bcf,
    sales_tax,
    congestion_surcharge,
    airport_fee,
    tip_amt,
    cbd_congestion_fee,
    total_amt,
    true_total_amt,
    comparation_total_amt,

    -- Flags de servicio
    shared_request_flag,
    shared_match_flag,
    access_a_ride_flag,
    wav_request_flag,
    wav_match_flag,

    -- Medidas derivadas
    EXTRACT(
        EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)
    ) / 60.0                                                        AS trip_duration_min,

    -- Tiempo de espera: desde que se solicitó hasta que llegó el conductor
    EXTRACT(
        EPOCH FROM (on_scene_datetime - request_datetime)
    ) / 60.0                                                        AS wait_time_min,

    -- Tarifa por milla
    CASE
        WHEN trip_distance > 0 THEN ROUND((fare_amount / trip_distance)::NUMERIC, 4)
        ELSE NULL
    END                                                             AS fare_per_mile,

    -- Ingreso total por minuto
    CASE
        WHEN trip_time > 0 THEN ROUND((true_total_amt / (trip_time / 60.0))::NUMERIC, 4)
        ELSE NULL
    END                                                             AS revenue_per_min,

    -- Flag: viaje con propina
    CASE WHEN tip_amt > 0 THEN TRUE ELSE FALSE END                  AS has_tip,

    -- Flag: viaje accesible (solicitado Y atendido con WAV)
    CASE WHEN wav_request_flag AND wav_match_flag THEN TRUE ELSE FALSE END AS is_wav_trip,

    -- Partición
    anio,
    mes

FROM {{ source('raw', 'formato_2') }}
