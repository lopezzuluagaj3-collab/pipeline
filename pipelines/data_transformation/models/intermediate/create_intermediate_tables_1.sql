-- =============================================================================
-- PROYECTO SIRIUS — Capa Intermediate en PostgreSQL
-- Schema: intermediate
-- Tipos de vehículo: green | fhv | hvfhs | yellow
-- Incluye: PRIMARY KEY + FOREIGN KEY constraints reales
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS intermediate;


-- =============================================================================
-- GREEN TAXI
-- Orden: primero todas las dims, luego la fact (que referencia las dims)
-- =============================================================================

CREATE TABLE intermediate.dim_green_datetime (
    datetime_id     TIMESTAMP   NOT NULL PRIMARY KEY,
    full_date       DATE        NOT NULL,
    year            INTEGER     NOT NULL,
    month           INTEGER     NOT NULL,
    day             INTEGER     NOT NULL,
    hour            INTEGER     NOT NULL,
    day_name        TEXT,
    month_abbr      TEXT,
    month_name      TEXT,
    day_of_week     INTEGER,
    week_of_year    INTEGER,
    quarter         INTEGER,
    is_weekend      BOOLEAN,
    season          TEXT,
    time_of_day     TEXT,
    is_rush_hour    BOOLEAN
);

CREATE TABLE intermediate.dim_green_location (
    location_id     INTEGER     NOT NULL PRIMARY KEY,
    zone_name       TEXT,
    borough         TEXT,
    service_zone    TEXT
);

CREATE TABLE intermediate.dim_green_payment_type (
    payment_type_id             INTEGER     NOT NULL PRIMARY KEY,
    payment_type_description    TEXT,
    is_electronic               BOOLEAN
);

CREATE TABLE intermediate.dim_green_ratecode (
    ratecode_id             INTEGER     NOT NULL PRIMARY KEY,
    ratecode_description    TEXT,
    ratecode_code           TEXT
);

CREATE TABLE intermediate.dim_green_trip_type (
    trip_type_id            INTEGER     NOT NULL PRIMARY KEY,
    trip_type_description   TEXT,
    trip_type_detail        TEXT
);

CREATE TABLE intermediate.dim_green_vendor (
    vendor_id           INTEGER     NOT NULL PRIMARY KEY,
    vendor_name         TEXT,
    vendor_short_name   TEXT
);

CREATE TABLE intermediate.fact_green_trips (
    trip_id                 TEXT            NOT NULL PRIMARY KEY,
    -- FKs → dimensiones
    pickup_datetime_id      TIMESTAMP       REFERENCES intermediate.dim_green_datetime(datetime_id),
    dropoff_datetime_id     TIMESTAMP       REFERENCES intermediate.dim_green_datetime(datetime_id),
    vendor_id               INTEGER         REFERENCES intermediate.dim_green_vendor(vendor_id),
    ratecode_id             INTEGER         REFERENCES intermediate.dim_green_ratecode(ratecode_id),
    pu_location_id          INTEGER         REFERENCES intermediate.dim_green_location(location_id),
    do_location_id          INTEGER         REFERENCES intermediate.dim_green_location(location_id),
    payment_type_id         INTEGER         REFERENCES intermediate.dim_green_payment_type(payment_type_id),
    trip_type_id            INTEGER         REFERENCES intermediate.dim_green_trip_type(trip_type_id),
    -- Timestamps exactos
    pickup_datetime         TIMESTAMP,
    dropoff_datetime        TIMESTAMP,
    -- Medidas de demanda
    passenger_count         INTEGER,
    trip_distance           NUMERIC(10, 4),
    -- Medidas de ingresos
    fare_amount             NUMERIC(10, 2),
    extra                   NUMERIC(10, 2),
    mta_tax                 NUMERIC(10, 2),
    tip_amt                 NUMERIC(10, 2),
    tolls_amt               NUMERIC(10, 2),
    congestion_surcharge    NUMERIC(10, 2),
    cbd_congestion_fee      NUMERIC(10, 2),
    improvement_surcharge   NUMERIC(10, 2),
    total_amt               NUMERIC(10, 2),
    true_total_amt          NUMERIC(10, 2),
    comparation_total_amt   NUMERIC(10, 2),
    -- Medidas derivadas
    trip_duration_min       NUMERIC(10, 4),
    fare_per_mile           NUMERIC(10, 4),
    revenue_per_min         NUMERIC(10, 4),
    -- Flags
    has_tip                 BOOLEAN,
    is_card_payment         BOOLEAN,
    -- Partición
    anio                    INTEGER,
    mes                     INTEGER
);


-- =============================================================================
-- FHV (For-Hire Vehicle)
-- =============================================================================

CREATE TABLE intermediate.dim_fhv_base (
    base_num    TEXT    NOT NULL PRIMARY KEY,
    base_name   TEXT
);

CREATE TABLE intermediate.dim_fhv_datetime (
    datetime_id     TIMESTAMP   NOT NULL PRIMARY KEY,
    full_date       DATE,
    year            INTEGER,
    month           INTEGER,
    day             INTEGER,
    hour            INTEGER,
    day_name        TEXT,
    month_abbr      TEXT,
    day_of_week     INTEGER,
    week_of_year    INTEGER,
    quarter         INTEGER,
    is_weekend      BOOLEAN,
    season          TEXT,
    time_of_day     TEXT,
    is_rush_hour    BOOLEAN
);

CREATE TABLE intermediate.fact_fhv_trips (
    trip_id                 TEXT        NOT NULL PRIMARY KEY,
    -- FKs → dimensiones
    pickup_datetime_id      TIMESTAMP   REFERENCES intermediate.dim_fhv_datetime(datetime_id),
    dropoff_datetime_id     TIMESTAMP   REFERENCES intermediate.dim_fhv_datetime(datetime_id),
    dispatching_base_num    TEXT        REFERENCES intermediate.dim_fhv_base(base_num),
    affiliated_base_number  TEXT        REFERENCES intermediate.dim_fhv_base(base_num),
    -- Timestamps exactos
    pickup_datetime         TIMESTAMP,
    dropoff_datetime        TIMESTAMP,
    -- FHV no tiene montos ni zonas
    trip_duration_min       NUMERIC(10, 4),
    -- Partición
    anio                    INTEGER,
    mes                     INTEGER
);


-- =============================================================================
-- HVFHS (Uber, Lyft, Via, Juno)
-- =============================================================================

CREATE TABLE intermediate.dim_hvfhs_datetime (
    datetime_id     TIMESTAMP   NOT NULL PRIMARY KEY,
    full_date       DATE,
    year            INTEGER,
    month           INTEGER,
    day             INTEGER,
    hour            INTEGER,
    day_name        TEXT,
    month_abbr      TEXT,
    day_of_week     INTEGER,
    week_of_year    INTEGER,
    quarter         INTEGER,
    is_weekend      BOOLEAN,
    season          TEXT,
    time_of_day     TEXT,
    is_rush_hour    BOOLEAN
);

CREATE TABLE intermediate.dim_hvfhs_license (
    hvfhs_license_num   TEXT        NOT NULL PRIMARY KEY,
    operator_name       TEXT,
    operator_detail     TEXT,
    is_active           BOOLEAN
);

CREATE TABLE intermediate.dim_hvfhs_location (
    location_id     INTEGER     NOT NULL PRIMARY KEY,
    zone_name       TEXT,
    borough         TEXT,
    service_zone    TEXT
);

CREATE TABLE intermediate.fact_hvfhs_trips (
    trip_id                 TEXT        NOT NULL PRIMARY KEY,
    -- FKs → dimensiones (hvfhs tiene 3 timestamps)
    request_datetime_id     TIMESTAMP   REFERENCES intermediate.dim_hvfhs_datetime(datetime_id),
    pickup_datetime_id      TIMESTAMP   REFERENCES intermediate.dim_hvfhs_datetime(datetime_id),
    dropoff_datetime_id     TIMESTAMP   REFERENCES intermediate.dim_hvfhs_datetime(datetime_id),
    hvfhs_license_num       TEXT        REFERENCES intermediate.dim_hvfhs_license(hvfhs_license_num),
    dispatching_base_num    TEXT,
    pu_location_id          INTEGER     REFERENCES intermediate.dim_hvfhs_location(location_id),
    do_location_id          INTEGER     REFERENCES intermediate.dim_hvfhs_location(location_id),
    -- Timestamps exactos
    request_datetime        TIMESTAMP,
    on_scene_datetime       TIMESTAMP,
    pickup_datetime         TIMESTAMP,
    dropoff_datetime        TIMESTAMP,
    -- Medidas
    trip_distance           NUMERIC(10, 4),
    trip_time_seconds       NUMERIC(10, 2),
    -- Ingresos
    fare_amount             NUMERIC(10, 2),
    tolls_amt               NUMERIC(10, 2),
    bcf                     NUMERIC(10, 2),
    sales_tax               NUMERIC(10, 2),
    congestion_surcharge    NUMERIC(10, 2),
    airport_fee             NUMERIC(10, 2),
    tip_amt                 NUMERIC(10, 2),
    cbd_congestion_fee      NUMERIC(10, 2),
    total_amt               NUMERIC(10, 2),
    true_total_amt          NUMERIC(10, 2),
    comparation_total_amt   NUMERIC(10, 2),
    -- Flags de servicio
    shared_request_flag     BOOLEAN,
    shared_match_flag       BOOLEAN,
    access_a_ride_flag      BOOLEAN,
    wav_request_flag        BOOLEAN,
    wav_match_flag          BOOLEAN,
    -- Medidas derivadas
    trip_duration_min       NUMERIC(10, 4),
    wait_time_min           NUMERIC(10, 4),
    fare_per_mile           NUMERIC(10, 4),
    revenue_per_min         NUMERIC(10, 4),
    -- Flags derivados
    has_tip                 BOOLEAN,
    is_wav_trip             BOOLEAN,
    -- Partición
    anio                    INTEGER,
    mes                     INTEGER
);


-- =============================================================================
-- YELLOW TAXI
-- =============================================================================

CREATE TABLE intermediate.dim_yellow_datetime (
    datetime_id     TIMESTAMP   NOT NULL PRIMARY KEY,
    full_date       DATE,
    year            INTEGER,
    month           INTEGER,
    day             INTEGER,
    hour            INTEGER,
    day_name        TEXT,
    month_abbr      TEXT,
    month_name      TEXT,
    day_of_week     INTEGER,
    week_of_year    INTEGER,
    quarter         INTEGER,
    is_weekend      BOOLEAN,
    season          TEXT,
    time_of_day     TEXT,
    is_rush_hour    BOOLEAN
);

CREATE TABLE intermediate.dim_yellow_location (
    location_id     INTEGER     NOT NULL PRIMARY KEY,
    zone_name       TEXT,
    borough         TEXT,
    service_zone    TEXT
);

CREATE TABLE intermediate.dim_yellow_payment_type (
    payment_type_id             INTEGER     NOT NULL PRIMARY KEY,
    payment_type_description    TEXT,
    is_electronic               BOOLEAN
);

CREATE TABLE intermediate.dim_yellow_ratecode (
    ratecode_id             INTEGER     NOT NULL PRIMARY KEY,
    ratecode_description    TEXT,
    ratecode_code           TEXT
);

CREATE TABLE intermediate.dim_yellow_vendor (
    vendor_id           INTEGER     NOT NULL PRIMARY KEY,
    vendor_name         TEXT,
    vendor_short_name   TEXT
);

CREATE TABLE intermediate.fact_yellow_trips (
    trip_id                 TEXT        NOT NULL PRIMARY KEY,
    -- FKs → dimensiones
    pickup_datetime_id      TIMESTAMP   REFERENCES intermediate.dim_yellow_datetime(datetime_id),
    dropoff_datetime_id     TIMESTAMP   REFERENCES intermediate.dim_yellow_datetime(datetime_id),
    vendor_id               INTEGER     REFERENCES intermediate.dim_yellow_vendor(vendor_id),
    ratecode_id             INTEGER     REFERENCES intermediate.dim_yellow_ratecode(ratecode_id),
    pu_location_id          INTEGER     REFERENCES intermediate.dim_yellow_location(location_id),
    do_location_id          INTEGER     REFERENCES intermediate.dim_yellow_location(location_id),
    payment_type_id         INTEGER     REFERENCES intermediate.dim_yellow_payment_type(payment_type_id),
    -- Timestamps exactos
    pickup_datetime         TIMESTAMP,
    dropoff_datetime        TIMESTAMP,
    -- Medidas de demanda
    passenger_count         INTEGER,
    trip_distance           NUMERIC(10, 4),
    -- Ingresos (Yellow: airport_fee + surcharge; sin improvement_surcharge)
    fare_amount             NUMERIC(10, 2),
    extra                   NUMERIC(10, 2),
    tip_amt                 NUMERIC(10, 2),
    tolls_amt               NUMERIC(10, 2),
    surcharge               NUMERIC(10, 2),
    mta_tax                 NUMERIC(10, 2),
    congestion_surcharge    NUMERIC(10, 2),
    airport_fee             NUMERIC(10, 2),
    cbd_congestion_fee      NUMERIC(10, 2),
    total_amt               NUMERIC(10, 2),
    true_total_amt          NUMERIC(10, 2),
    comparation_total_amt   BOOLEAN,        -- ⚠ Yellow es BOOLEAN, no NUMERIC
    -- Medidas derivadas
    trip_duration_min       NUMERIC(10, 4),
    fare_per_mile           NUMERIC(10, 4),
    revenue_per_min         NUMERIC(10, 4),
    -- Flags
    has_tip                 BOOLEAN,
    is_card_payment         BOOLEAN,
    is_airport_trip         BOOLEAN,
    -- Partición
    anio                    INTEGER,
    mes                     INTEGER
);
