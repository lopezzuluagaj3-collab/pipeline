{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    partition_by=['anio', 'mes'],
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/fct_viajes',
    pre_hook=[
      "SET spark.sql.sources.partitionOverwriteMode = dynamic",
      "SET spark.sql.shuffle.partitions = 600",
      "SET spark.sql.adaptive.enabled = true"
    ]
  )
}}

-- ─────────────────────────────────────────────
-- Tabla de hechos: un registro por viaje (FHVHV)
-- FKs -> dim_fecha, dim_zona, dim_compania, dim_base_despacho
-- Incremental: en cada corrida sobrescribe solo la partición anio/mes
-- ─────────────────────────────────────────────
-- El staging se materializa particionado por ruta física en S3
-- (.../anio=YYYY/mes=MM/stg_hvfhs/), por lo que se lee directamente por ruta
-- en vez de con ref(). anio/mes vienen como columnas dentro del parquet.
-- En modo incremental se apunta solo a la partición del mes (poda de archivos);
-- en full-refresh se leen todas con glob.
with viajes as (

    select *
    from parquet.`s3a://sirius-logs-riwi/tlc/staging/fhvhv/
        {%- if is_incremental() -%}
            anio={{ var('anio') }}/mes={{ '%02d' | format(var('mes') | int) }}/stg_hvfhs/
        {%- else -%}
            anio=*/mes=*/stg_hvfhs/
        {%- endif -%}`

)

select
    -- ── Clave del hecho (dimensión degenerada / surrogate) ──
    {{ dbt_utils.generate_surrogate_key([
        'hvfhs_license_num',
        'dispatching_base_num',
        'request_datetime',
        'tpep_pickup_datetime',
        'pu_location_id',
        'do_location_id'
    ]) }}                                                            as viaje_key,

    -- ── Claves foráneas a dimensiones ──
    hvfhs_license_num,                                                -- FK -> dim_compania
    coalesce(dispatching_base_num, 'DESCONOCIDA')                    as dispatching_base_num,  -- FK -> dim_base_despacho
    coalesce(pu_location_id, -1)                                     as pu_location_id,        -- FK -> dim_zona (origen)
    coalesce(do_location_id, -1)                                     as do_location_id,        -- FK -> dim_zona (destino)
    cast(date_format(request_datetime,       'yyyyMMdd') as integer) as request_fecha_key,     -- FK -> dim_fecha
    cast(date_format(tpep_pickup_datetime,   'yyyyMMdd') as integer) as pickup_fecha_key,      -- FK -> dim_fecha
    cast(date_format(tpep_dropoff_datetime,  'yyyyMMdd') as integer) as dropoff_fecha_key,     -- FK -> dim_fecha

    -- ── Marcas de tiempo (dimensiones degeneradas) ──
    request_datetime,
    on_scene_datetime,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,

    -- ── Flags (dimensiones degeneradas) ──
    shared_request_flag,
    shared_match_flag,
    access_a_ride_flag,
    wav_request_flag,
    wav_match_flag,
    comparation_total_amt,

    -- ── Métricas aditivas ──
    -- OJO (diccionario TLC): total_amt = driver_pay = pago NETO al conductor
    -- (sin peajes ni propinas, neto de comisión). NO es lo que pagó el pasajero.
    -- true_total_amt = suma de cargos al pasajero (tarifa + peajes + bcf + tax +
    -- recargos + aeropuerto + propina + cbd). Son magnitudes distintas.
    trip_distance,
    trip_time,
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

    -- ── Métricas derivadas de tiempo (segundos) ──
    -- Diccionario TLC: on_scene_datetime SOLO se registra en vehículos
    -- accesibles (WAV). En staging se imputó con pickup_datetime cuando venía
    -- nulo, por lo que las métricas basadas en on_scene solo son fiables cuando
    -- wav_match_flag = true; para el resto se dejan en NULL (no en 0 engañoso).
    unix_timestamp(tpep_pickup_datetime)  - unix_timestamp(request_datetime)        as tiempo_solicitud_recogida_seg,
    unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)    as duracion_viaje_seg,
    case when wav_match_flag
         then unix_timestamp(on_scene_datetime) - unix_timestamp(request_datetime)
    end                                                                             as tiempo_arribo_conductor_seg,
    case when wav_match_flag
         then unix_timestamp(tpep_pickup_datetime) - unix_timestamp(on_scene_datetime)
    end                                                                             as tiempo_espera_sitio_seg,

    -- ── Partición ──
    anio,
    mes
from viajes
