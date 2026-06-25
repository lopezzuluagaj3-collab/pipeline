{{
  config(
    materialized = 'table',
    schema       = 'yellow_intermediate',
    tags         = ['yellow', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Vendedor / Proveedor del taxi Yellow
  Idéntica a green — mismos dos proveedores TLC.
*/

SELECT 1 AS vendor_id, 'Creative Mobile Technologies' AS vendor_name, 'CMT' AS vendor_short_name
UNION ALL
SELECT 2,               'VeriFone Inc.',                                'VTS'
UNION ALL
SELECT 0,               'Unknown',                                      'UNK'
