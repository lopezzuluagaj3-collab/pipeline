{{
  config(
    materialized = 'table',
    schema       = 'yellow_intermediate',
    tags         = ['yellow', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Tipo de pago para Yellow Taxi
  Idéntica a green — mismo diccionario TLC.
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
