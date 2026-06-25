{{
  config(
    materialized = 'table',
    schema       = 'fhv_intermediate',
    tags         = ['fhv', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Bases TLC de FHV
  Construida con los números de base únicos que aparecen en los datos.
  dispatching_base_num = base que despacha el viaje
  affiliated_base_number = base a la que pertenece el vehículo
*/

WITH all_bases AS (
    SELECT DISTINCT dispatching_base_num AS base_num FROM {{ source('raw', 'formato_1') }} WHERE dispatching_base_num IS NOT NULL
    UNION
    SELECT DISTINCT affiliated_base_number             FROM {{ source('raw', 'formato_1') }} WHERE affiliated_base_number IS NOT NULL
)

SELECT
    base_num,
    'TLC Base ' || base_num AS base_name
FROM all_bases
ORDER BY base_num
