{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/dim_base_despacho'
  )
}}

-- ─────────────────────────────────────────────
-- Dimensión de base de despacho
-- No existe lookup oficial: se construye con los códigos
-- distintos observados en toda la fuente staging.
-- Ajusta el ref('stg_fhvhv') al nombre real de tu modelo staging.
-- ─────────────────────────────────────────────
with bases as (

    select distinct
        dispatching_base_num
    from {{ ref('stg_fhvhv') }}
    where dispatching_base_num is not null

)

select
    dispatching_base_num   -- PK (clave natural)
from bases

union all

select 'DESCONOCIDA' as dispatching_base_num
