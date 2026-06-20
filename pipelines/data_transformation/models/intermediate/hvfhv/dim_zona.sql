{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/dim_zona'
  )
}}

-- ─────────────────────────────────────────────
-- Dimensión de zona (TLC Taxi Zone Lookup)
-- Sirve tanto para PU (pickup) como para DO (dropoff)
-- Carga el seed taxi_zone_lookup.csv (265 zonas oficiales)
-- ─────────────────────────────────────────────
with lookup as (

    select * from {{ ref('taxi_zone_lookup') }}

),

zonas as (

    select
        cast(locationid as integer)     as location_id,   -- PK
        cast(borough as string)         as municipio,
        cast(zone as string)            as zona,
        cast(service_zone as string)    as zona_servicio
    from lookup

)

select location_id, municipio, zona, zona_servicio
from zonas

union all

-- Miembro "Desconocido" para FKs sin coincidencia (location_id nulo -> -1)
select
    -1            as location_id,
    'Desconocido' as municipio,
    'Desconocido' as zona,
    'Desconocido' as zona_servicio
