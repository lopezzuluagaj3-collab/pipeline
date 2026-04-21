from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


REQUIRED_COLUMNS = [
    "ordernumber",
    "orderlinenumber",
    "orderdate",
    "quantityordered",
    "priceeach",
    "sales",
    "status",
    "productline",
    "msrp",
    "productcode",
    "customername",
    "city",
    "country",
    "dealsize",
]


def build_connection_string() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "retailco_db")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"


def build_engine():
    return create_engine(build_connection_string(), future=True)


@contextmanager
def build_connection():
    engine = build_engine()
    with engine.begin() as connection:
        yield connection


def extraer(ruta: str) -> pd.DataFrame:
    return pd.read_csv(ruta, encoding='latin-1')


def transformar(df: pd.DataFrame) -> pd.DataFrame:
    work_df = df.copy()
    work_df.columns = [column.strip().lower() for column in work_df.columns]

    for column in REQUIRED_COLUMNS:
        if column not in work_df.columns:
            work_df[column] = None

    work_df = work_df[REQUIRED_COLUMNS].drop_duplicates(subset=["ordernumber", "orderlinenumber"])
    work_df["orderdate"] = pd.to_datetime(work_df["orderdate"], errors="coerce")
    work_df["quantityordered"] = pd.to_numeric(work_df["quantityordered"], errors="coerce").fillna(0).astype(int)
    for column in ["priceeach", "sales", "msrp"]:
        work_df[column] = pd.to_numeric(work_df[column], errors="coerce").fillna(0.0)

    for column in ["status", "productline", "productcode", "customername", "city", "country", "dealsize"]:
        work_df[column] = work_df[column].fillna("UNKNOWN").astype(str).str.strip()

    work_df["customer_id"] = (
        work_df["customername"].str.upper()
        + "|"
        + work_df["city"].str.upper()
        + "|"
        + work_df["country"].str.upper()
    )
    return work_df.dropna(subset=["orderdate"]).reset_index(drop=True)


def cargar(df: pd.DataFrame, conn) -> None:
    if df.empty:
        return

    schema_path = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"
    conn.execute(text(schema_path.read_text(encoding="utf-8")))

    fechas = (
        df.assign(
            fecha=df["orderdate"].dt.date,
            fecha_key=df["orderdate"].dt.strftime("%Y%m%d").astype(int),
            anio=df["orderdate"].dt.year.astype(int),
            trimestre=df["orderdate"].dt.quarter.astype(int),
            mes=df["orderdate"].dt.month.astype(int),
            nombre_mes=df["orderdate"].dt.month_name(),
            dia=df["orderdate"].dt.day.astype(int),
            dia_semana_num=df["orderdate"].dt.isocalendar().day.astype(int),
            nombre_dia_semana=df["orderdate"].dt.day_name(),
        )[
            [
                "fecha_key",
                "fecha",
                "anio",
                "trimestre",
                "mes",
                "nombre_mes",
                "dia",
                "dia_semana_num",
                "nombre_dia_semana",
            ]
        ]
        .drop_duplicates()
    )

    productos = df[["productcode", "productline", "msrp"]].drop_duplicates()
    clientes = (
        df[["customer_id", "customername", "city", "country"]]
        .rename(columns={"customer_id": "cliente_id", "customername": "nombre", "city": "ciudad", "country": "pais"})
        .drop_duplicates()
    )
    paises = (
        df[["country"]]
        .rename(columns={"country": "country_normalized"})
        .assign(
            capital="UNKNOWN",
            region="UNKNOWN",
            subregion="UNKNOWN",
            currency="UNKNOWN",
            country_code2="UN",
            country_code3="UNK",
        )
        .drop_duplicates()
    )

    for _, row in fechas.iterrows():
        conn.execute(
            text(
                """
                INSERT INTO dim_fecha (
                    fecha_key, fecha, anio, trimestre, mes, nombre_mes, dia, dia_semana_num, nombre_dia_semana
                ) VALUES (
                    :fecha_key, :fecha, :anio, :trimestre, :mes, :nombre_mes, :dia, :dia_semana_num, :nombre_dia_semana
                )
                ON CONFLICT DO NOTHING
                """
            ),
            row.to_dict(),
        )

    for _, row in productos.iterrows():
        conn.execute(
            text(
                """
                INSERT INTO dim_producto (productcode, productline, msrp)
                VALUES (:productcode, :productline, :msrp)
                ON CONFLICT DO NOTHING
                """
            ),
            row.to_dict(),
        )

    for _, row in clientes.iterrows():
        conn.execute(
            text(
                """
                INSERT INTO dim_cliente (cliente_id, nombre, ciudad, pais)
                VALUES (:cliente_id, :nombre, :ciudad, :pais)
                ON CONFLICT DO NOTHING
                """
            ),
            row.to_dict(),
        )

    for _, row in paises.iterrows():
        conn.execute(
            text(
                """
                INSERT INTO dim_pais_region (
                    country_normalized, capital, region, subregion, currency, country_code2, country_code3
                ) VALUES (
                    :country_normalized, :capital, :region, :subregion, :currency, :country_code2, :country_code3
                )
                ON CONFLICT DO NOTHING
                """
            ),
            row.to_dict(),
        )

    producto_lookup = pd.read_sql("SELECT producto_key, productcode FROM dim_producto", conn)
    pais_lookup = pd.read_sql("SELECT pais_key, country_normalized FROM dim_pais_region", conn)
    facts = df.merge(producto_lookup, on="productcode", how="left").merge(
        pais_lookup, left_on="country", right_on="country_normalized", how="left"
    )
    facts["fecha_key"] = facts["orderdate"].dt.strftime("%Y%m%d").astype(int)

    for _, row in facts.iterrows():
        conn.execute(
            text(
                """
                INSERT INTO fact_ventas (
                    ordernumber, orderlinenumber, fecha_key, producto_key, cliente_id, pais_key,
                    quantityordered, priceeach, sales, status, dealsize
                ) VALUES (
                    :ordernumber, :orderlinenumber, :fecha_key, :producto_key, :cliente_id, :pais_key,
                    :quantityordered, :priceeach, :sales, :status, :dealsize
                )
                ON CONFLICT DO NOTHING
                """
            ),
            {
                "ordernumber": int(row["ordernumber"]),
                "orderlinenumber": int(row["orderlinenumber"]),
                "fecha_key": int(row["fecha_key"]),
                "producto_key": int(row["producto_key"]),
                "cliente_id": row["customer_id"],
                "pais_key": int(row["pais_key"]),
                "quantityordered": int(row["quantityordered"]),
                "priceeach": float(row["priceeach"]),
                "sales": float(row["sales"]),
                "status": row["status"],
                "dealsize": row["dealsize"],
            },
        )
