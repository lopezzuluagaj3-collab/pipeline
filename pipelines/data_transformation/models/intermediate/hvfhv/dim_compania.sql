{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/dim_compania'
  )
}}

-- ─────────────────────────────────────────────
-- Dimensión de compañía (mapeo oficial del código HVFHS)
-- Fuente: diccionario de datos TLC FHVHV
-- ─────────────────────────────────────────────
with companias as (

    select 'HV0002' as hvfhs_license_num, 'Juno' as compania
    union all select 'HV0003', 'Uber'
    union all select 'HV0004', 'Via'
    union all select 'HV0005', 'Lyft'
    -- Miembro desconocido por robustez
    union all select 'DESCONOCIDO', 'Desconocida'

)

select
    hvfhs_license_num,   -- PK
    compania
from companias
