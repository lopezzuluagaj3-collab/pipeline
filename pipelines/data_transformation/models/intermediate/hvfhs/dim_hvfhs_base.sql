{{ config(
    materialized = 'table',
    schema       = 'hvfhs_intermediate'
) }}

/*
  Dimensión de bases despachantes HVFHS.
  Fuente: staging.hvfhs → dispatching_base_num
*/

WITH source AS (

    SELECT DISTINCT
        UPPER(TRIM(dispatching_base_num)) AS dispatching_base_num
    FROM {{ source('staging', 'hvfhs') }}
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
