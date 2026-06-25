{{ config(
    materialized = 'table',
    schema       = 'hvfhs_intermediate'
) }}

/*
  Dimensión de licencias HVFHS (operadores de alto volumen).
  Enriquece el código de licencia con el nombre comercial del operador
  según el diccionario de datos TLC (vigente desde Sep 2019).
*/

WITH codigos_licencia AS (

    SELECT DISTINCT hvfhs_license_num
    FROM {{ source('staging', 'hvfhs') }}
    WHERE hvfhs_license_num IS NOT NULL

),

mapeo_operador (hvfhs_license_num, nombre_operador) AS (

    VALUES
        ('HV0002', 'Juno'),
        ('HV0003', 'Uber'),
        ('HV0004', 'Via'),
        ('HV0005', 'Lyft')

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY c.hvfhs_license_num) AS license_key,
        c.hvfhs_license_num,
        COALESCE(m.nombre_operador, 'Desconocido')        AS nombre_operador
    FROM codigos_licencia c
    LEFT JOIN mapeo_operador m USING (hvfhs_license_num)

)

SELECT * FROM final
