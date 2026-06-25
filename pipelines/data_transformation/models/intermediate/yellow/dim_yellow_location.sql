{{
  config(
    materialized = 'table',
    schema       = 'yellow_intermediate',
    tags         = ['yellow', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Zona geográfica TLC para Yellow Taxi
  Misma lógica que green y hvfhs.

  Nota histórica: Los datos de Yellow de 2009-2010 usaban coordenadas GPS
  en lugar de location IDs. En el staging se les asignaron zonas del top
  Manhattan (161, 162, 230, 236, 237) o 264 (Unknown) para normalizar.
  Eso ya está resuelto — aquí solo se mapean los IDs que llegaron.
*/

WITH all_locations AS (
    SELECT DISTINCT pu_location_id AS location_id FROM {{ source('raw', 'formato_4') }} WHERE pu_location_id IS NOT NULL
    UNION
    SELECT DISTINCT do_location_id             FROM {{ source('raw', 'formato_4') }} WHERE do_location_id IS NOT NULL
),

known_zones (location_id, zone_name, borough, service_zone) AS (
    VALUES
        (1,   'Newark Airport',                  'EWR',       'Airports'),
        (132, 'JFK Airport',                     'Queens',    'Airports'),
        (138, 'LaGuardia Airport',               'Queens',    'Airports'),
        (161, 'Midtown Center',                  'Manhattan', 'Yellow Zone'),
        (162, 'Midtown East',                    'Manhattan', 'Yellow Zone'),
        (230, 'Times Sq/Theatre District',       'Manhattan', 'Yellow Zone'),
        (236, 'Upper East Side North',           'Manhattan', 'Yellow Zone'),
        (237, 'Upper East Side South',           'Manhattan', 'Yellow Zone'),
        (264, 'Unknown',                         'Unknown',   'Unknown'),
        (265, 'Unknown',                         'Unknown',   'Unknown')
)

SELECT
    l.location_id,
    COALESCE(k.zone_name,    'Zone ' || l.location_id::VARCHAR) AS zone_name,
    COALESCE(k.borough,      'Unknown')                          AS borough,
    COALESCE(k.service_zone, 'Yellow Zone')                      AS service_zone
FROM all_locations l
LEFT JOIN known_zones k USING (location_id)
ORDER BY l.location_id
