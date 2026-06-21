{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/dim_base_despacho'
  )
}}

-- ─────────────────────────────────────────────
-- Dimensión de base de despacho
-- No existe lookup oficial: se construye con los códigos distintos observados
-- en TODO el staging. Se lee por ruta (glob) porque el staging está
-- particionado físicamente en S3 y la tabla del metastore solo apunta a la
-- última partición escrita.
-- ─────────────────────────────────────────────
with bases as (

    select distinct
        dispatching_base_num
    from parquet.`s3a://sirius-logs-riwi/tlc/staging/fhvhv/anio=*/mes=*/stg_hvfhs/`
    where dispatching_base_num is not null

)

select
    dispatching_base_num   -- PK (clave natural)
from bases

union all

select 'DESCONOCIDA' as dispatching_base_num
