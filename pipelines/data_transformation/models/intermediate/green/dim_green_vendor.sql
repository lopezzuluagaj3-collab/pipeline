{{
  config(
    materialized = 'table',
    schema       = 'green_intermediate',
    tags         = ['green', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Vendedor / Proveedor del taxi
  Fuente: Diccionario de datos del TLC de NYC
*/

SELECT 1 AS vendor_id, 'Creative Mobile Technologies' AS vendor_name, 'CMT'    AS vendor_short_name
UNION ALL
SELECT 2,               'VeriFone Inc.',                                'VTS'
UNION ALL
SELECT 0,               'Unknown',                                      'UNK'   -- para datos sin vendor
