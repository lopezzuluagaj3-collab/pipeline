{{
  config(
    materialized = 'table',
    schema       = 'hvfhs_intermediate',
    tags         = ['hvfhs', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Zona geográfica TLC para HVFHS
  Misma lógica que green — zonas especiales conocidas hardcodeadas,
  el resto se nombra genéricamente.
*/

WITH all_locations AS (
    SELECT DISTINCT pu_location_id AS location_id FROM {{ source('raw', 'formato_2') }} WHERE pu_location_id IS NOT NULL
    UNION
    SELECT DISTINCT do_location_id             FROM {{ source('raw', 'formato_2') }} WHERE do_location_id IS NOT NULL
),

known_zones (location_id, zone_name, borough, service_zone) AS (
    VALUES
        (1,   'Newark Airport',   'EWR',     'Airports'),
        (132, 'JFK Airport',      'Queens',  'Airports'),
        (138, 'LaGuardia Airport','Queens',  'Airports'),
        (264, 'Unknown',          'Unknown', 'Unknown'),
        (265, 'Unknown',          'Unknown', 'Unknown')
)

SELECT
    l.location_id,
    COALESCE(k.zone_name,    'Zone ' || l.location_id::VARCHAR) AS zone_name,
    COALESCE(k.borough,      'Unknown')                          AS borough,
    COALESCE(k.service_zone, 'Boro Zone')                        AS service_zone
FROM all_locations l
LEFT JOIN known_zones k USING (location_id)
ORDER BY l.location_id
