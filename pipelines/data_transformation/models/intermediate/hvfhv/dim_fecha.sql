{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/intermediate/dim_fecha'
  )
}}

-- ─────────────────────────────────────────────
-- Dimensión de fecha (conformada)
-- Cubre todo el rango de negocio: 2019-01-01 .. 2026-12-31
-- Requiere el paquete dbt_utils
-- ─────────────────────────────────────────────
with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2019-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}

),

fechas as (

    select cast(date_day as date) as fecha
    from spine

)

select
    cast(date_format(fecha, 'yyyyMMdd') as integer)             as fecha_key,   -- PK (YYYYMMDD)
    fecha,
    year(fecha)                                                 as anio,
    quarter(fecha)                                              as trimestre,
    month(fecha)                                                as mes,
    date_format(fecha, 'MMMM')                                  as nombre_mes,
    day(fecha)                                                  as dia,
    weekofyear(fecha)                                           as semana_anio,
    dayofweek(fecha)                                            as dia_semana_num,  -- 1=Domingo
    date_format(fecha, 'EEEE')                                  as nombre_dia,
    case when dayofweek(fecha) in (1, 7) then true else false end as es_fin_semana
from fechas
