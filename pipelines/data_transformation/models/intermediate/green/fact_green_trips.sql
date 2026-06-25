{{
  config(
    materialized = 'table',
    schema       = 'green_intermediate',
    tags         = ['green', 'intermediate', 'fact']
  )
}}

/*
  Tabla de hechos: Viajes Green Taxi NYC
  ----------------------------------------
  Esquema estrella — tabla central.

  Granularidad: 1 fila = 1 viaje individual.

  FKs a dimensiones:
    - pickup_datetime_id  → dim_green_datetime.datetime_id
    - dropoff_datetime_id → dim_green_datetime.datetime_id
    - vendor_id           → dim_green_vendor.vendor_id
    - ratecode_id         → dim_green_ratecode.ratecode_id
    - pu_location_id      → dim_green_location.location_id
    - do_location_id      → dim_green_location.location_id
    - payment_type_id     → dim_green_payment_type.payment_type_id
    - trip_type_id        → dim_green_trip_type.trip_type_id

  Medidas:
    - Contables directas: distancia, pasajeros, montos
    - Derivadas: duración en minutos, tarifa por milla
*/

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'formato_3') }}
)

SELECT
    -- -------------------------------------------------------------------------
    -- Surrogate key del viaje (MD5 sobre las columnas que lo identifican)
    -- -------------------------------------------------------------------------
    MD5(
        COALESCE(vendor_id::TEXT,                '') || '|' ||
        COALESCE(tpep_pickup_datetime::TEXT,     '') || '|' ||
        COALESCE(tpep_dropoff_datetime::TEXT,    '') || '|' ||
        COALESCE(pu_location_id::TEXT,           '') || '|' ||
        COALESCE(do_location_id::TEXT,           '')
    )                                                               AS trip_id,

    -- -------------------------------------------------------------------------
    -- Foreign Keys → Dimensiones
    -- -------------------------------------------------------------------------
    DATE_TRUNC('hour', tpep_pickup_datetime)                        AS pickup_datetime_id,
    DATE_TRUNC('hour', tpep_dropoff_datetime)                       AS dropoff_datetime_id,

    vendor_id,
    ratecode_id,
    pu_location_id,
    do_location_id,
    payment_type                                                    AS payment_type_id,
    trip_type                                                       AS trip_type_id,

    -- -------------------------------------------------------------------------
    -- Timestamps exactos (no truncados) — para análisis de duración precisa
    -- -------------------------------------------------------------------------
    tpep_pickup_datetime                                            AS pickup_datetime,
    tpep_dropoff_datetime                                           AS dropoff_datetime,

    -- -------------------------------------------------------------------------
    -- Medidas de demanda
    -- -------------------------------------------------------------------------
    passenger_count,
    trip_distance,

    -- -------------------------------------------------------------------------
    -- Medidas de ingresos (desglose de la tarifa)
    -- -------------------------------------------------------------------------
    fare_amount,
    extra,
    mta_tax,
    tip_amt,
    tolls_amt,
    congestion_surcharge,
    cbd_congestion_fee,
    improvement_surcharge,
    total_amt,
    true_total_amt,
    comparation_total_amt,

    -- -------------------------------------------------------------------------
    -- Medidas derivadas
    -- -------------------------------------------------------------------------

    -- Duración del viaje en minutos
    EXTRACT(
        EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)
    ) / 60.0                                                        AS trip_duration_min,

    -- Tarifa por milla (evita división por cero)
    CASE
        WHEN trip_distance > 0 THEN ROUND((fare_amount / trip_distance)::NUMERIC, 4)
        ELSE NULL
    END                                                             AS fare_per_mile,

    -- Tarifa total por minuto
    CASE
        WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 0
        THEN ROUND(
                (true_total_amt / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0))::NUMERIC,
             4)
        ELSE NULL
    END                                                             AS revenue_per_min,

    -- Flag: viaje con propina
    CASE WHEN tip_amt > 0 THEN TRUE ELSE FALSE END                  AS has_tip,

    -- Flag: viaje pagado electrónicamente (credit card)
    CASE WHEN payment_type = 1 THEN TRUE ELSE FALSE END             AS is_card_payment,

    -- -------------------------------------------------------------------------
    -- Columnas de partición (heredadas del staging para facilitar análisis)
    -- -------------------------------------------------------------------------
    anio,
    mes

FROM source
