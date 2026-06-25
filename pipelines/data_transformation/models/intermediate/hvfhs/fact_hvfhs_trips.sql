{{ config(
    materialized = 'table',
    schema       = 'hvfhs_intermediate'
) }}

/*
  Tabla de hechos de viajes HVFHS (High Volume For-Hire Services).

  Granularidad: 1 fila = 1 viaje HVFHS.
  Medidas:      distancia, tiempo, tarifas, cargos adicionales, propinas, totales.
  Derived:      wait_time_minutes (request → pickup), trip_duration_minutes.
  Flags:        shared_request, shared_match, access_a_ride, wav_request, wav_match.
  Claves FK:    license_key, base_key, pickup_date_key, dropoff_date_key,
                pu_location_id, do_location_id.
*/

WITH source AS (

    SELECT
        hvfhs_license_num,
        dispatching_base_num,
        request_datetime,
        on_scene_datetime,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        pu_location_id,
        do_location_id,
        trip_distance,
        trip_time,
        fare_amount,
        tolls_amt,
        bcf,
        sales_tax,
        congestion_surcharge,
        airport_fee,
        tip_amt,
        total_amt,
        shared_request_flag,
        shared_match_flag,
        access_a_ride_flag,
        wav_request_flag,
        wav_match_flag,
        cbd_congestion_fee,
        true_total_amt,
        comparation_total_amt,
        anio,
        mes
    FROM {{ source('staging', 'hvfhs') }}
    WHERE hvfhs_license_num    IS NOT NULL
      AND tpep_pickup_datetime  IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL

),

dim_license AS (

    SELECT license_key, hvfhs_license_num
    FROM {{ ref('dim_hvfhs_license') }}

),

dim_base AS (

    SELECT base_key, dispatching_base_num
    FROM {{ ref('dim_hvfhs_base') }}

),

enriquecido AS (

    SELECT
        -- Claves de dimensión
        l.license_key,
        b.base_key,
        TO_CHAR(DATE(s.tpep_pickup_datetime),  'YYYYMMDD')::INTEGER  AS pickup_date_key,
        TO_CHAR(DATE(s.tpep_dropoff_datetime), 'YYYYMMDD')::INTEGER  AS dropoff_date_key,
        s.pu_location_id,
        s.do_location_id,

        -- Timestamps completos
        s.request_datetime,
        s.on_scene_datetime,
        s.tpep_pickup_datetime   AS pickup_datetime,
        s.tpep_dropoff_datetime  AS dropoff_datetime,

        -- Atributos de hora (granularidad intra-día)
        EXTRACT(HOUR FROM s.tpep_pickup_datetime)::INTEGER   AS pickup_hour,
        EXTRACT(HOUR FROM s.tpep_dropoff_datetime)::INTEGER  AS dropoff_hour,

        -- Métricas temporales derivadas
        ROUND(
            EXTRACT(EPOCH FROM (s.tpep_pickup_datetime - s.request_datetime)) / 60.0,
            2
        )                                                     AS wait_time_minutes,

        ROUND(
            EXTRACT(EPOCH FROM (s.tpep_dropoff_datetime - s.tpep_pickup_datetime)) / 60.0,
            2
        )                                                     AS trip_duration_minutes,

        -- Métricas de viaje
        s.trip_distance,
        s.trip_time                                           AS trip_time_seconds,

        -- Componentes financieros
        s.fare_amount,
        s.tolls_amt,
        s.bcf,
        s.sales_tax,
        s.congestion_surcharge,
        s.airport_fee,
        s.tip_amt,
        s.cbd_congestion_fee,
        s.total_amt,
        s.true_total_amt,
        s.comparation_total_amt,

        -- Flags de servicio
        s.shared_request_flag,
        s.shared_match_flag,
        s.access_a_ride_flag,
        s.wav_request_flag,
        s.wav_match_flag,

        -- Partición
        s.anio,
        s.mes

    FROM source s
    LEFT JOIN dim_license l
           ON s.hvfhs_license_num = l.hvfhs_license_num
    LEFT JOIN dim_base b
           ON UPPER(TRIM(s.dispatching_base_num)) = b.dispatching_base_num

),

final AS (

    SELECT
        ROW_NUMBER() OVER (
            ORDER BY pickup_date_key, license_key, base_key, pickup_datetime
        )                   AS trip_id,
        *
    FROM enriquecido

)

SELECT * FROM final
