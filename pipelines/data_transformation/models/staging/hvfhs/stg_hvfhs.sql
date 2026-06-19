{{
  config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://sirius-logs-riwi/tlc/staging/fhvhv/anio=' ~ var("anio") ~ '/mes=' ~ "%02d" | format(var("mes") | int),
    pre_hook=[
      "CREATE OR REPLACE TEMPORARY VIEW fhv_source USING parquet OPTIONS (path 's3a://sirius-logs-riwi/tlc/raw/fhvhv/" ~ var("anio") ~ "/fhvtripdata" ~ var("anio") ~ "-" ~ "%02d" | format(var("mes") | int) ~ ".parquet')"
    ]
  )
}}

-- ─────────────────────────────────────────────
-- CTE 1: Leer el archivo parquet de la fuente FHVHV
-- ─────────────────────────────────────────────
with source as (

    select *
    from fhvhv_source

),

-- ─────────────────────────────────────────────
-- CTE 2: Renombrar, castear columnas y aplicar Coalesce básicos
-- Excluye: originating_base_num (Regla 4)
-- ─────────────────────────────────────────────
renamed as (

    select
        cast(hvfhs_license_num as varchar)                                      as hvfhs_license_num,
        cast(dispatching_base_num as varchar)                                   as dispatching_base_num,
        cast(request_datetime as timestamp)                                     as request_datetime,
        -- Regla 3: Si on_scene_datetime es NULL, reemplazar con pickup_datetime
        cast(coalesce(on_scene_datetime, pickup_datetime) as timestamp)         as on_scene_datetime,
        cast(pickup_datetime as timestamp)                                      as tpep_pickup_datetime,
        cast(dropoff_datetime as timestamp)                                     as tpep_dropoff_datetime,
        cast(pulocationid as integer)                                           as pu_location_id,
        cast(dolocationid as integer)                                           as do_location_id,
        cast(trip_miles as double)                                              as trip_distance,
        cast(trip_time as integer)                                              as trip_time,
        
        -- Regla 2: Reemplazo de nulos monetarios por 0
        cast(coalesce(base_passenger_fare, 0.0) as double)                      as fare_amount,
        cast(coalesce(tolls, 0.0) as double)                                    as tolls_amt,
        cast(coalesce(bcf, 0.0) as double)                                      as bcf,
        cast(coalesce(sales_tax, 0.0) as double)                                as sales_tax,
        cast(coalesce(congestion_surcharge, 0.0) as double)                     as congestion_surcharge,
        cast(coalesce(airport_fee, 0.0) as double)                              as airport_fee,
        cast(coalesce(tips, 0.0) as double)                                     as tip_amt,
        cast(coalesce(cbd_congestion_fee, 0.0) as double)                       as cbd_congestion_fee,
        cast(coalesce(driver_pay, 0.0) as double)                               as total_amt,
        
        -- Regla 5: Reemplazo de nulos en flags booleanos por FALSE
        cast(coalesce(shared_request_flag, 'N') in ('Y', 'true', '1') as boolean) as shared_request_flag,
        cast(coalesce(shared_match_flag, 'N') in ('Y', 'true', '1') as boolean)   as shared_match_flag,
        cast(coalesce(access_a_ride_flag, 'N') in ('Y', 'true', '1') as boolean)  as access_a_ride_flag,
        cast(coalesce(wav_request_flag, 'N') in ('Y', 'true', '1') as boolean)    as wav_request_flag,
        cast(coalesce(wav_match_flag, 'N') in ('Y', 'true', '1') as boolean)      as wav_match_flag

    from source

),

-- ─────────────────────────────────────────────
-- CTE 3: Filtrar solo registros del año correcto
-- ─────────────────────────────────────────────
filtrado_anio as (

    select *
    from renamed
    where year(tpep_pickup_datetime) = {{ var("anio") }}

),

-- ─────────────────────────────────────────────
-- CTE 4: Eliminar duplicados con DISTINCT (Regla 1)
-- Evita problemas de OOM en Workers pequeños
-- ─────────────────────────────────────────────
sin_duplicados as (

    select distinct *
    from filtrado_anio

),

-- ─────────────────────────────────────────────
-- CTE 5: Validaciones Temporales y Negativos (Reglas 6, 8 y 9)
-- ─────────────────────────────────────────────
valida_filtros as (

    select *
    from sin_duplicados
    where 
        -- Regla 6: Validaciones temporales
        tpep_pickup_datetime <= tpep_dropoff_datetime
        and tpep_pickup_datetime >= on_scene_datetime
        and tpep_pickup_datetime >= request_datetime
        
        -- Regla 8: Valores negativos
        and total_amt >= 0
        and trip_distance >= 0
        
        -- Regla 9: Duración anómala (24 horas = 86400 segundos o usando epoch/dates)
        -- DuckDB permite restar timestamps obteniendo un INTERVAL o usar epoch
        and epoch(tpep_dropoff_datetime) - epoch(tpep_pickup_datetime) > 0
        and epoch(tpep_dropoff_datetime) - epoch(tpep_pickup_datetime) <= 86400

),

-- ─────────────────────────────────────────────
-- CTE 6: Calcular los percentiles 99.9% mediante Window Functions
-- Preparación para el tratamiento de Outliers (Regla 7)
-- ─────────────────────────────────────────────
calculo_percentiles as (

    select *,
        percentile_cont(fare_amount, 0.999) over ()          as p999_fare_amount,
        percentile_cont(tolls_amt, 0.999) over ()             as p999_tolls_amt,
        percentile_cont(bcf, 0.999) over ()                   as p999_bcf,
        percentile_cont(sales_tax, 0.999) over ()             as p999_sales_tax,
        percentile_cont(congestion_surcharge, 0.999) over () as p999_congestion_surcharge,
        percentile_cont(airport_fee, 0.999) over ()           as p999_airport_fee,
        percentile_cont(tip_amt, 0.999) over ()               as p999_tip_amt,
        percentile_cont(cbd_congestion_fee, 0.999) over ()    as p999_cbd_congestion_fee,
        percentile_cont(total_amt, 0.999) over ()             as p999_total_amt
    from valida_filtros

),

-- ─────────────────────────────────────────────
-- CTE 7: Reemplazar Outliers (Regla 7)
-- Si supera P99.9 + 50%, se capa a ese valor máximo tolerado
-- ─────────────────────────────────────────────
tratamiento_outliers as (

    select
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
        shared_request_flag,
        shared_match_flag,
        access_a_ride_flag,
        wav_request_flag,
        wav_match_flag,

        case when fare_amount > (p999_fare_amount * 1.5) then (p999_fare_amount * 1.5) else fare_amount end as fare_amount,
        case when tolls_amt > (p999_tolls_amt * 1.5) then (p999_tolls_amt * 1.5) else tolls_amt end as tolls_amt,
        case when bcf > (p999_bcf * 1.5) then (p999_bcf * 1.5) else bcf end as bcf,
        case when sales_tax > (p999_sales_tax * 1.5) then (p999_sales_tax * 1.5) else sales_tax end as sales_tax,
        case when congestion_surcharge > (p999_congestion_surcharge * 1.5) then (p999_congestion_surcharge * 1.5) else congestion_surcharge end as congestion_surcharge,
        case when airport_fee > (p999_airport_fee * 1.5) then (p999_airport_fee * 1.5) else airport_fee end as airport_fee,
        case when tip_amt > (p999_tip_amt * 1.5) then (p999_tip_amt * 1.5) else tip_amt end as tip_amt,
        case when cbd_congestion_fee > (p999_cbd_congestion_fee * 1.5) then (p999_cbd_congestion_fee * 1.5) else cbd_congestion_fee end as cbd_congestion_fee,
        case when total_amt > (p999_total_amt * 1.5) then (p999_total_amt * 1.5) else total_amt end as total_amt

    from calculo_percentiles

),

-- ─────────────────────────────────────────────
-- CTE 8: Columnas Derivadas (true_total_amt y comparation_total_amt)
-- ─────────────────────────────────────────────
columnas_derivadas as (

    select 
        *,
        cast((
            fare_amount
            + tolls_amt
            + bcf
            + sales_tax
            + congestion_surcharge
            + airport_fee
            + tip_amt
            + cbd_congestion_fee
        ) as double) as true_total_amt
    from tratamiento_outliers

),

-- ─────────────────────────────────────────────
-- CTE 9: Formateo y Estructura Final con Partición
-- ─────────────────────────────────────────────
final as (

    select
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
        -- Columnas derivadas finales
        cast(case when true_total_amt != total_amt then true or else false end as boolean) as comparation_total_amt,
        -- Columnas de control de partición S3
        cast({{ var("anio") }} as integer) as anio,
        cast({{ var("mes") }}  as integer) as mes
    from columnas_derivadas

)

select * from final