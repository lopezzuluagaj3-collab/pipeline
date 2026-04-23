CREATE TABLE IF NOT EXISTS dim_fecha (
    fecha_key INTEGER PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    anio INTEGER NOT NULL,
    trimestre INTEGER NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    mes INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    nombre_mes VARCHAR(20) NOT NULL,
    dia INTEGER NOT NULL CHECK (dia BETWEEN 1 AND 31),
    dia_semana_num INTEGER NOT NULL CHECK (dia_semana_num BETWEEN 1 AND 7),
    nombre_dia_semana VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_producto (
    producto_key BIGSERIAL PRIMARY KEY,
    productcode VARCHAR(50) NOT NULL UNIQUE,
    productline VARCHAR(100),
    msrp NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS dim_cliente (
    cliente_id TEXT PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    ciudad VARCHAR(120),
    pais VARCHAR(120)
);

CREATE TABLE IF NOT EXISTS dim_pais_region (
    pais_key BIGSERIAL PRIMARY KEY,
    country_normalized VARCHAR(120) NOT NULL UNIQUE,
    capital VARCHAR(120),
    region VARCHAR(80),
    subregion VARCHAR(120),
    currency VARCHAR(120),
    country_code2 VARCHAR(2),
    country_code3 VARCHAR(3)
);

CREATE TABLE IF NOT EXISTS fact_ventas (
    venta_key BIGSERIAL PRIMARY KEY,
    ordernumber INTEGER NOT NULL,
    orderlinenumber INTEGER NOT NULL,
    fecha_key INTEGER NOT NULL REFERENCES dim_fecha(fecha_key),
    producto_key BIGINT NOT NULL REFERENCES dim_producto(producto_key),
    cliente_id TEXT NOT NULL REFERENCES dim_cliente(cliente_id),
    pais_key BIGINT NOT NULL REFERENCES dim_pais_region(pais_key),
    quantityordered INTEGER,
    priceeach NUMERIC(12,2),
    sales NUMERIC(14,2),
    status VARCHAR(50),
    dealsize VARCHAR(30),
    CONSTRAINT uq_fact_ventas_grano UNIQUE (ordernumber, orderlinenumber)
);

CREATE INDEX IF NOT EXISTS idx_fact_ventas_fecha ON fact_ventas (fecha_key);
CREATE INDEX IF NOT EXISTS idx_fact_ventas_producto ON fact_ventas (producto_key);
CREATE INDEX IF NOT EXISTS idx_fact_ventas_cliente ON fact_ventas (cliente_id);
CREATE INDEX IF NOT EXISTS idx_fact_ventas_pais ON fact_ventas (pais_key);
