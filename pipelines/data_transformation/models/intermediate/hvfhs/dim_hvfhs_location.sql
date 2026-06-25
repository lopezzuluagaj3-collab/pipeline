{{ config(
    materialized = 'table',
    schema       = 'hvfhs_intermediate'
) }}

/*
  Dimensión de zonas de recogida/destino HVFHS.
  Combina los IDs de ubicación presentes en los viajes con la tabla de
  referencia taxi_zone_lookup (seed) para agregar Borough, Zone y service_zone.
*/

WITH ids_en_uso AS (

    SELECT DISTINCT pu_location_id AS location_id FROM {{ source('staging', 'hvfhs') }} WHERE pu_location_id IS NOT NULL
    UNION
    SELECT DISTINCT do_location_id             FROM {{ source('staging', 'hvfhs') }} WHERE do_location_id IS NOT NULL

),

zone_lookup AS (

    SELECT
        "LocationID"::INTEGER AS location_id,
        "Borough"             AS borough,
        "Zone"                AS zone,
        "service_zone"        AS service_zone
    FROM {{ ref('taxi_zone_lookup') }}

),

final AS (

    SELECT
        i.location_id,
        COALESCE(z.borough,      'Unknown') AS borough,
        COALESCE(z.zone,         'Unknown') AS zone,
        COALESCE(z.service_zone, 'Unknown') AS service_zone
    FROM ids_en_uso i
    LEFT JOIN zone_lookup z USING (location_id)

)

SELECT * FROM final
ORDER BY location_id
