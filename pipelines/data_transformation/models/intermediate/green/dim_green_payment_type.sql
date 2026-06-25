{{
  config(
    materialized = 'table',
    schema       = 'green_intermediate',
    tags         = ['green', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Tipo de pago del pasajero
  Fuente: Diccionario de datos del TLC de NYC
*/

SELECT 1 AS payment_type_id, 'Credit card'  AS payment_type_description, TRUE  AS is_electronic
UNION ALL
SELECT 2,                    'Cash',                                       FALSE
UNION ALL
SELECT 3,                    'No charge',                                  FALSE
UNION ALL
SELECT 4,                    'Dispute',                                    FALSE
UNION ALL
SELECT 5,                    'Unknown',                                    FALSE
UNION ALL
SELECT 6,                    'Voided trip',                                FALSE
