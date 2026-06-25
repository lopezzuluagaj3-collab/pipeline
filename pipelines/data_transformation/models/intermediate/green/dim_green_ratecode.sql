{{
  config(
    materialized = 'table',
    schema       = 'green_intermediate',
    tags         = ['green', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Código de tarifa aplicada al viaje
  Fuente: Diccionario de datos del TLC de NYC
*/

SELECT 1  AS ratecode_id, 'Standard rate'          AS ratecode_description, 'STD'  AS ratecode_code
UNION ALL
SELECT 2,                 'JFK',                                             'JFK'
UNION ALL
SELECT 3,                 'Newark',                                          'EWR'
UNION ALL
SELECT 4,                 'Nassau or Westchester',                           'NAS'
UNION ALL
SELECT 5,                 'Negotiated fare',                                 'NEG'
UNION ALL
SELECT 6,                 'Group ride',                                      'GRP'
UNION ALL
SELECT 99,                'Unknown',                                         'UNK'
