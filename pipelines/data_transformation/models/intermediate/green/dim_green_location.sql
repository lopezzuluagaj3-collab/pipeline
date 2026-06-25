{{
  config(
    materialized = 'table',
    schema       = 'green_intermediate',
    tags         = ['green', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Zona geográfica (TLC Taxi Zone)
  Se construye con todos los location_id que aparecen en los datos.

  NOTA: Si en el futuro tienen la tabla de zonas del TLC disponible en Postgres
  (taxi_zones con location_id, zone, borough, service_zone), pueden reemplazar
  este modelo haciendo JOIN con ella para enriquecer con nombre real de zona y borough.

  Por ahora se deja con los IDs conocidos del diccionario TLC para los boroughs,
  y el resto como 'Unknown zone X'.
*/

WITH all_locations AS (
    SELECT DISTINCT pu_location_id AS location_id FROM {{ source('raw', 'formato_3') }} WHERE pu_location_id IS NOT NULL
    UNION
    SELECT DISTINCT do_location_id             FROM {{ source('raw', 'formato_3') }} WHERE do_location_id IS NOT NULL
),

-- Zonas especiales conocidas del TLC de NYC
known_zones (location_id, zone_name, borough, service_zone) AS (
    VALUES
        (1,   'Newark Airport',             'EWR',          'Airports'),
        (132, 'JFK Airport',                'Queens',       'Airports'),
        (138, 'LaGuardia Airport',          'Queens',       'Airports'),
        (264, 'Unknown',                    'Unknown',      'Unknown'),
        (265, 'Unknown',                    'Unknown',      'Unknown')
)

SELECT
    l.location_id,
    COALESCE(k.zone_name,    'Zone ' || l.location_id::VARCHAR) AS zone_name,
    COALESCE(k.borough,      'Unknown')                          AS borough,
    COALESCE(k.service_zone, 'Boro Zone')                        AS service_zone
FROM all_locations l
LEFT JOIN known_zones k USING (location_id)
ORDER BY l.location_id
