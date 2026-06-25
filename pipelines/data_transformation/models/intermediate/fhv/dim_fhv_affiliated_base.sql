{{ config(
    materialized = 'table',
    schema       = 'fhv_intermediate'
) }}

/*
  Dimensión de bases afiliadas FHV.
  Fuente: staging.fhv → affiliated_base_number
*/

WITH source AS (

    SELECT DISTINCT
        UPPER(TRIM(affiliated_base_number)) AS affiliated_base_number
    FROM {{ source('staging', 'fhv') }}
    WHERE affiliated_base_number IS NOT NULL
      AND TRIM(affiliated_base_number) <> ''

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY affiliated_base_number) AS affiliated_base_key,
        affiliated_base_number
    FROM source

)

SELECT * FROM final
