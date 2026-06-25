{{
  config(
    materialized = 'table',
    schema       = 'hvfhs_intermediate',
    tags         = ['hvfhs', 'intermediate', 'dimension']
  )
}}

/*
  Dimensión: Operador HVFHS (High Volume For-Hire Service)
  Tabla estática — diccionario oficial del TLC de NYC.

  Estos son los operadores de apps de transporte regulados como HVFHS
  por procesar más de 10,000 viajes por día.
*/

SELECT 'HV0002' AS hvfhs_license_num, 'Juno'  AS operator_name, 'Juno (adquirida por Lyft)'  AS operator_detail, FALSE AS is_active
UNION ALL
SELECT 'HV0003',                      'Uber',                   'Uber Technologies Inc.',      TRUE
UNION ALL
SELECT 'HV0004',                      'Via',                    'Via Transportation Inc.',     TRUE
UNION ALL
SELECT 'HV0005',                      'Lyft',                   'Lyft Inc.',                   TRUE
