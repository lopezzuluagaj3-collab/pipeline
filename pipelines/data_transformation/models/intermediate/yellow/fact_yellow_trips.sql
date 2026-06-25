{{
  config(
    materialized = 'table',
    schema       = 'yellow_intermediate',
    tags         = ['yellow', 'intermediate', 'fact']
  )
}}

/*
  Tabla de hechos: Viajes Yellow Taxi NYC
  ----------------------------------------
  Yellow es el tipo de taxi más antiguo e icónico de NYC.
  Datos disponibles desde 2009.

  Diferencias vs Green:
    - Tiene airport_fee  (cargo aeropuerto JFK/LaGuardia)
    - Tiene surcharge    (improvement surcharge, $0.30)
    - NO tiene trip_type (exclusivo de green)
    - comparation_total_amt es BOOLEAN (TRUE = montos coinciden dentro de $0.01)

  Granularidad: 1 fila = 1 viaje.

  FKs a dimensiones:
    - pickup_datetime_id  → dim_yellow_datetime.datetime_id
    - dropoff_datetime_id → dim_yellow_datetime.datetime_id
    - vendor_id           → dim_yellow_vendor.vendor_id
    - ratecode_id         → dim_yellow_ratecode.ratecode_id
    - pu_location_id      → dim_yellow_location.location_id
    - do_location_id      → dim_yellow_location.location_id
    - payment_type_id     → dim_yellow_payment_type.payment_type_id
*/

SELECT
    -- Surrogate key
    MD5(
        COALESCE(vendor_id::TEXT,                '') || '|' ||
        COALESCE(tpep_pickup_datetime::TEXT,     '') || '|' ||
        COALESCE(tpep_dropoff_datetime::TEXT,    '') || '|' ||
        COALESCE(pu_location_id::TEXT,           '') || '|' ||
        COALESCE(do_location_id::TEXT,           '')
    )                                                               AS trip_id,

    -- FKs → Dimensiones
    DATE_TRUNC('hour', tpep_pickup_datetime)                        AS pickup_datetime_id,
    DATE_TRUNC('hour', tpep_dropoff_datetime)                       AS dropoff_datetime_id,
    vendor_id,
    ratecode_id,
    pu_location_id,
    do_location_id,
    payment_type                                                    AS payment_type_id,

    -- Timestamps exactos
    tpep_pickup_datetime                                            AS pickup_datetime,
    tpep_dropoff_datetime                                           AS dropoff_datetime,

    -- Medidas de demanda
    passenger_count,
    trip_distance,

    -- Medidas de ingresos (desglose completo Yellow)
    fare_amount,
    extra,
    tip_amt,
    tolls_amt,
    surcharge,                  -- improvement surcharge $0.30
    mta_tax,
    congestion_surcharge,
    airport_fee,                -- exclusivo Yellow: JFK/LaGuardia fee
    cbd_congestion_fee,
    total_amt,
    true_total_amt,
    comparation_total_amt,      -- BOOLEAN: TRUE si montos coinciden (diferencia < $0.01)

    -- Medidas derivadas
    EXTRACT(
        EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)
    ) / 60.0                                                        AS trip_duration_min,

    -- Tarifa por milla
    CASE
        WHEN trip_distance > 0 THEN ROUND((fare_amount / trip_distance)::NUMERIC, 4)
        ELSE NULL
    END                                                             AS fare_per_mile,

    -- Ingreso total por minuto
    CASE
        WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 0
        THEN ROUND(
                (true_total_amt / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0))::NUMERIC,
             4)
        ELSE NULL
    END                                                             AS revenue_per_min,

    -- Flags
    CASE WHEN tip_amt > 0    THEN TRUE ELSE FALSE END               AS has_tip,
    CASE WHEN payment_type = 1 THEN TRUE ELSE FALSE END             AS is_card_payment,
    CASE WHEN airport_fee > 0 THEN TRUE ELSE FALSE END              AS is_airport_trip,

    -- Partición
    anio,
    mes

FROM {{ source('raw', 'formato_4') }}
