{{ config(
    materialized = 'table',
    schema       = 'fhv_intermediate'
) }}

/*
  Dimensión de bases despachantes FHV.
  Fuente: staging.fhv → dispatching_base_num
*/

WITH source AS (

    SELECT DISTINCT
        UPPER(TRIM(dispatching_base_num)) AS dispatching_base_num
    FROM {{ source('staging', 'fhv') }}
    WHERE dispatching_base_num IS NOT NULL
      AND TRIM(dispatching_base_num) <> ''

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY dispatching_base_num) AS base_key,
        dispatching_base_num
    FROM source

)

SELECT * FROM final
