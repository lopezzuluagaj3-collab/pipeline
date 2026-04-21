

SET search_path TO emausoft_dw, public;

BEGIN;


DROP TABLE IF EXISTS stg_clientes;
CREATE TEMP TABLE stg_clientes (
    cliente_id TEXT,
    nombre TEXT,
    ciudad TEXT,
    pais TEXT
);

DROP TABLE IF EXISTS stg_regiones;
CREATE TEMP TABLE stg_regiones (
    country_normalized TEXT,
    capital TEXT,
    region TEXT,
    subregion TEXT,
    currency TEXT,
    country_code2 TEXT,
    country_code3 TEXT
);

DROP TABLE IF EXISTS stg_ventas;
CREATE TEMP TABLE stg_ventas (
    ordernumber TEXT,
    quantityordered TEXT,
    priceeach TEXT,
    orderlinenumber TEXT,
    sales TEXT,
    orderdate TEXT,
    status TEXT,
    qtr_id TEXT,
    month_id TEXT,
    year_id TEXT,
    productline TEXT,
    msrp TEXT,
    productcode TEXT,
    customername TEXT,
    phone TEXT,
    addressline1 TEXT,
    city TEXT,
    postalcode TEXT,
    country TEXT,
    contactlastname TEXT,
    contactfirstname TEXT,
    dealsize TEXT,
    country_normalized TEXT,
    capital TEXT,
    region TEXT,
    subregion TEXT,
    currency TEXT,
    country_code2 TEXT,
    country_code3 TEXT,
    region_match TEXT,
    cliente_id TEXT,
    nombre TEXT,
    ciudad TEXT,
    pais TEXT,
    pais_normalized TEXT,
    cliente_match TEXT
);


\copy stg_clientes FROM './.venv/data/new_data/clean_data_client.csv' WITH (FORMAT csv, HEADER true)
\copy stg_regiones FROM './.venv/data/new_data/clean_data_regiones.csv' WITH (FORMAT csv, HEADER true)
\copy stg_ventas   FROM './.venv/data/new_data/ventas_con_clientes.csv' WITH (FORMAT csv, HEADER true)


INSERT INTO dim_cliente (cliente_id, nombre, ciudad, pais)
SELECT
    cliente_id::INTEGER,
    NULLIF(TRIM(nombre), ''),
    NULLIF(TRIM(ciudad), ''),
    NULLIF(TRIM(pais), '')
FROM stg_clientes
WHERE NULLIF(TRIM(cliente_id), '') IS NOT NULL
ON CONFLICT (cliente_id) DO UPDATE
SET
    nombre = EXCLUDED.nombre,
    ciudad = EXCLUDED.ciudad,
    pais = EXCLUDED.pais;

INSERT INTO dim_pais_region (
    country_normalized,
    capital,
    region,
    subregion,
    currency,
    country_code2,
    country_code3
)
SELECT
    NULLIF(TRIM(country_normalized), ''),
    NULLIF(TRIM(capital), ''),
    NULLIF(TRIM(region), ''),
    NULLIF(TRIM(subregion), ''),
    NULLIF(TRIM(currency), ''),
    NULLIF(TRIM(country_code2), ''),
    NULLIF(TRIM(country_code3), '')
FROM stg_regiones
WHERE NULLIF(TRIM(country_normalized), '') IS NOT NULL
ON CONFLICT (country_normalized) DO UPDATE
SET
    capital = EXCLUDED.capital,
    region = EXCLUDED.region,
    subregion = EXCLUDED.subregion,
    currency = EXCLUDED.currency,
    country_code2 = EXCLUDED.country_code2,
    country_code3 = EXCLUDED.country_code3;

INSERT INTO dim_producto (productcode, productline, msrp)
SELECT DISTINCT
    NULLIF(TRIM(productcode), '') AS productcode,
    NULLIF(TRIM(productline), '') AS productline,
    NULLIF(TRIM(msrp), '')::NUMERIC(12,2) AS msrp
FROM stg_ventas
WHERE NULLIF(TRIM(productcode), '') IS NOT NULL
ON CONFLICT (productcode) DO UPDATE
SET
    productline = EXCLUDED.productline,
    msrp = EXCLUDED.msrp;

INSERT INTO dim_fecha (
    fecha_key,
    fecha,
    anio,
    trimestre,
    mes,
    nombre_mes,
    dia,
    dia_semana_num,
    nombre_dia_semana
)
SELECT DISTINCT
    TO_CHAR(ts::DATE, 'YYYYMMDD')::INTEGER AS fecha_key,
    ts::DATE AS fecha,
    EXTRACT(YEAR FROM ts)::INTEGER AS anio,
    EXTRACT(QUARTER FROM ts)::INTEGER AS trimestre,
    EXTRACT(MONTH FROM ts)::INTEGER AS mes,
    TO_CHAR(ts, 'Month')::VARCHAR(20) AS nombre_mes,
    EXTRACT(DAY FROM ts)::INTEGER AS dia,
    EXTRACT(ISODOW FROM ts)::INTEGER AS dia_semana_num,
    TO_CHAR(ts, 'Day')::VARCHAR(20) AS nombre_dia_semana
FROM (
    SELECT TO_TIMESTAMP(TRIM(orderdate), 'FMMM/FMDD/YYYY HH24:MI') AS ts
    FROM stg_ventas
    WHERE NULLIF(TRIM(orderdate), '') IS NOT NULL
) x
ON CONFLICT (fecha_key) DO NOTHING;


INSERT INTO fact_ventas (
    ordernumber,
    orderlinenumber,
    fecha_key,
    producto_key,
    cliente_id,
    pais_key,
    quantityordered,
    priceeach,
    sales,
    status,
    dealsize,
    qtr_id,
    month_id,
    year_id
)
SELECT
    v.ordernumber::INTEGER,
    v.orderlinenumber::INTEGER,
    TO_CHAR(TO_TIMESTAMP(TRIM(v.orderdate), 'FMMM/FMDD/YYYY HH24:MI')::DATE, 'YYYYMMDD')::INTEGER AS fecha_key,
    p.producto_key,
    COALESCE(v.cliente_id::INTEGER, -1) AS cliente_id,
    COALESCE(r.pais_key, unk.pais_key) AS pais_key,
    NULLIF(TRIM(v.quantityordered), '')::INTEGER,
    NULLIF(TRIM(v.priceeach), '')::NUMERIC(12,2),
    NULLIF(TRIM(v.sales), '')::NUMERIC(14,2),
    NULLIF(TRIM(v.status), ''),
    NULLIF(TRIM(v.dealsize), ''),
    NULLIF(TRIM(v.qtr_id), '')::INTEGER,
    NULLIF(TRIM(v.month_id), '')::INTEGER,
    NULLIF(TRIM(v.year_id), '')::INTEGER
FROM stg_ventas v
JOIN dim_producto p
    ON p.productcode = NULLIF(TRIM(v.productcode), '')
LEFT JOIN dim_pais_region r
    ON r.country_normalized = NULLIF(TRIM(v.country_normalized), '')
JOIN dim_pais_region unk
    ON unk.country_normalized = 'UNKNOWN'
WHERE NULLIF(TRIM(v.ordernumber), '') IS NOT NULL
  AND NULLIF(TRIM(v.orderlinenumber), '') IS NOT NULL
  AND NULLIF(TRIM(v.orderdate), '') IS NOT NULL
ON CONFLICT (ordernumber, orderlinenumber) DO NOTHING;

COMMIT;


SELECT 'dim_fecha' AS tabla, COUNT(*) AS filas FROM dim_fecha
UNION ALL
SELECT 'dim_producto', COUNT(*) FROM dim_producto
UNION ALL
SELECT 'dim_cliente', COUNT(*) FROM dim_cliente
UNION ALL
SELECT 'dim_pais_region', COUNT(*) FROM dim_pais_region
UNION ALL
SELECT 'fact_ventas', COUNT(*) FROM fact_ventas;
