{{
  config(
    materialized = 'table',
    schema       = 'green_intermediate',
    tags         = ['green', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Tipo de viaje (cómo se solicitó el taxi)
  Solo aplica a Green taxi (no Yellow)
  Fuente: Diccionario de datos del TLC de NYC
*/

SELECT 1 AS trip_type_id, 'Street-hail' AS trip_type_description, 'Pasajero paró el taxi en la calle'    AS trip_type_detail
UNION ALL
SELECT 2,                 'Dispatch',                               'Viaje solicitado por despacho (app)'
