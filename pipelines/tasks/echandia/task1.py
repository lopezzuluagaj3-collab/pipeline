# ============================================================
# RetailCo — Ejercicio 6: Pipeline ETL optimizado
# Usa executemany para insertar en batch — mucho más rápido
# ============================================================
from io import StringIO, BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import psycopg2
import os
import sys
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
from airflow.decorators import task

load_dotenv()

DB_CONFIG = {
    "host":     "51.222.142.204",
    "port":     "5432",
    "dbname":   "retailco_db_jp",
    "user":     "coder-ra-c6",
    "password": "Riwi2026**",
}

DB_URI = f"postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["dbname"]}"
engine_pg = create_engine(DB_URI)

def log(mensaje):
    hora = datetime.now().strftime("%H:%M:%S")
    print(f"[{hora}] {mensaje}")

def copy_chunk(df: pd.DataFrame, table: str):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        f"COPY {table} FROM STDIN WITH CSV",
        buffer
    )

    conn.commit()
    cur.close()
    conn.close()

def insert_multi(df: pd.DataFrame, table: str):

    df.to_sql(
        table,
        engine_pg,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10000
    )

# ============================================================
# FUNCIÓN 1 — EXTRAER
# ============================================================
@task(queue="test1")
def extraer(bucket_name: str, file_key: str, conn_id: str = "aws_default") -> pd.DataFrame:
    log(f"EXTRAER → Conectando a S3 (Bucket: {bucket_name}, Archivo: {file_key})")

    # 1. Instanciar el Hook de Airflow
    s3_hook = S3Hook(aws_conn_id=conn_id)

    # 2. Verificar si el archivo existe en el bucket
    if not s3_hook.check_for_key(file_key, bucket_name):
        log(f"ERROR: El archivo {file_key} no existe en el bucket {bucket_name}")
        sys.exit(1)

    # 3. Descargar el archivo a memoria
    archivo_obj = s3_hook.get_key(file_key, bucket_name)
    contenido = archivo_obj.get()['Body'].read()

    # 4. Cargar en Pandas
    # Usamos io.BytesIO para que pandas lo lea como si fuera un archivo local
    df = pd.read_csv(BytesIO(contenido), encoding="utf-8", on_bad_lines="skip")

    log(f"EXTRAER → Registros entrantes desde S3: {len(df):,}")
    return df


# ============================================================
# FUNCIÓN 2 — TRANSFORMAR
# ============================================================
@task(queue="test2")
def transformar(df: pd.DataFrame) -> pd.DataFrame:
    log("TRANSFORMAR → Iniciando limpieza...")

    df = df.dropna(subset=["Order ID"])
    df = df.drop_duplicates(subset=["Order ID"], keep="first")
    df = df.dropna(subset=["Amount"])

    df["Qty"] = df["Qty"].fillna(0)
    for col in ["ship-city", "ship-state", "ship-country", "ship-service-level"]:
        df[col] = df[col].fillna("Desconocido")
    for col in ["Category", "Size", "Style"]:
        df[col] = df[col].fillna("Sin clasificar")

    df["Date"] = pd.to_datetime(df["Date"], format="%m-%d-%y", errors="coerce")
    df = df.dropna(subset=["Date"])

    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
    df["Qty"]    = pd.to_numeric(df["Qty"],    errors="coerce").fillna(0).astype(int)

    df["mes"]             = df["Date"].dt.month
    df["semana_del_anio"] = df["Date"].dt.isocalendar().week.astype(int)
    df["trimestre"]       = df["Date"].dt.quarter
    df["anio"]            = df["Date"].dt.year
    df["dia"]             = df["Date"].dt.day
    df["nombre_mes"]      = df["Date"].dt.strftime("%B")

    df["ticket_promedio"] = df.apply(
        lambda row: round(row["Amount"] / row["Qty"], 2) if row["Qty"] > 0 else row["Amount"],
        axis=1
    )

    if "Unnamed: 22;" in df.columns:
        df = df.drop(columns=["Unnamed: 22;"])

    log(f"TRANSFORMAR → Registros limpios: {len(df):,}")
    return df

# ============================================================
# FUNCIÓN 3 — CARGAR (todo en batch)
# ============================================================
@task(queue="test3")
def cargar(df: pd.DataFrame, conn) -> dict:
    log("CARGAR → Iniciando carga en batch...")
    cur = conn.cursor()

    # ── dim_tiempo ───────────────────────────────────────────
    log("CARGAR → dim_tiempo...")
    fechas = df[["Date","dia","mes","nombre_mes","trimestre","anio","semana_del_anio"]]\
               .drop_duplicates(subset=["Date"])
    fechas = fechas.rename(columns={"Date": "fecha"})
    # batch_tiempo = [
    #     (row["Date"].date(), int(row["dia"]), int(row["mes"]),
    #      row["nombre_mes"], int(row["trimestre"]),
    #      int(row["anio"]), int(row["semana_del_anio"]))
    #     for _, row in fechas.iterrows()
    # ]
    insert_multi(fechas, "dim_tiempo")
    # cur.executemany("""
    #     INSERT INTO dim_tiempo (fecha,dia,mes,nombre_mes,trimestre,anio,semana_del_anio)
    #     VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (fecha) DO NOTHING
    # """, batch_tiempo)
    # conn.commit()
    log(f"CARGAR → dim_tiempo: {len(fechas)} fechas")

    # ── dim_producto ─────────────────────────────────────────
    log("CARGAR → dim_producto...")
    productos = df[["SKU","Style","Category","Size"]].drop_duplicates(subset=["SKU"])
    productos = productos.rename(columns={ "SKU": "sku", "Category": "categoria", "Size": "talla", "Style": "estilo" })
    batch_prod_clean = productos[["sku", "categoria", "talla", "estilo"]].apply(lambda x: x.str.strip()).replace('', np.nan)
    batch_prod = batch_prod_clean.dropna(subset=["sku"])
    # batch_prod = [
    #     (str(r["SKU"]).strip(), str(r["SKU"]).strip(),
    #      str(r["Category"]), str(r["Size"]), str(r["Style"]))
    #     for _, r in productos.iterrows()
    #     if pd.notna(r["SKU"]) and str(r["SKU"]).strip() != ""
    # ]
    insert_multi(batch_prod, "dim_producto")
    # cur.executemany("""
    #     INSERT INTO dim_producto (sku,nombre,categoria,talla,estilo)
    #     VALUES (%s,%s,%s,%s,%s) ON CONFLICT (sku) DO NOTHING
    # """, batch_prod)
    # conn.commit()
    log(f"CARGAR → dim_producto: {len(batch_prod)} SKUs")

    # ── dim_cliente ──────────────────────────────────────────
    log("CARGAR → dim_cliente...")
    clientes = df[["ship-city","ship-state","ship-country"]].drop_duplicates()
    clientes.insert(0, "nombre_destinatario", "Desconocido")
    batch_cli = clientes.rename(columns={"ship-city": "ciudad_destino", "ship-state": "estado_destino", "ship-country": "pais_destino"})
    insert_multi(batch_cli, "dim_cliente")
    # cur.executemany("""
    #     INSERT INTO dim_cliente (nombre_destinatario,ciudad_destino,estado_destino,pais_destino)
    #     VALUES (%s,%s,%s,%s)
    # """, batch_cli)
    # conn.commit()
    log(f"CARGAR → dim_cliente: {len(batch_cli)} registros")

    # ── dim_envio ────────────────────────────────────────────
    log("CARGAR → dim_envio...")
    envios = df[["ship-service-level","ship-state","ship-city","ship-country","Status"]]\
               .drop_duplicates()

    batch_env = envios.rename(columns={
        "ship-service-level": "ship_service_level",
        "ship-state": "ship_state",
        "ship-city": "ship_city",
        "ship-country": "ship_country",
        "Status": "estado_envio"
    })

    batch_env["estado_envio"] = batch_env["estado_envio"].replace(np.nan, "Desconocido")
    #batch_env = [
    #    (str(r["ship-service-level"]), str(r["ship-state"]),
    #     str(r["ship-city"]), str(r["ship-country"]),
    #     str(r["Status"]) if pd.notna(r["Status"]) else "Desconocido")
    #    for _, r in envios.iterrows()
    #]
    insert_multi(batch_env, "dim_envio")
    # cur.executemany("""
    #     INSERT INTO dim_envio (ship_service_level,ship_state,ship_city,ship_country,estado_envio)
    #     VALUES (%s,%s,%s,%s,%s)
    # """, batch_env)
    # conn.commit()
    log(f"CARGAR → dim_envio: {len(batch_env)} registros")

    # ── Mapas de IDs ─────────────────────────────────────────
    cur.execute("SELECT fecha, tiempo_id FROM dim_tiempo")
    map_tiempo = {str(r[0]): r[1] for r in cur.fetchall()}

    cur.execute("SELECT sku, producto_id FROM dim_producto")
    map_producto = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("SELECT cliente_id, ciudad_destino, estado_destino FROM dim_cliente")
    map_cliente = {(r[1], r[2]): r[0] for r in cur.fetchall()}

    cur.execute("SELECT envio_id, ship_service_level, ship_state, ship_city FROM dim_envio")
    map_envio = {(r[1], r[2], r[3]): r[0] for r in cur.fetchall()}

    # ── fact_ventas en batch ──────────────────────────────────
    log("CARGAR → fact_ventas (batch)...")
    batch_fact = []
    omitidos = 0

    for _, row in df.iterrows():
        tiempo_id   = map_tiempo.get(str(row["Date"].date()))
        producto_id = map_producto.get(str(row["SKU"]).strip())
        cliente_id  = map_cliente.get((str(row["ship-city"]), str(row["ship-state"])))
        envio_id    = map_envio.get((str(row["ship-service-level"]),
                                     str(row["ship-state"]), str(row["ship-city"])))

        if not tiempo_id or not producto_id:
            omitidos += 1
            continue

        batch_fact.append((
            str(row["Order ID"]), tiempo_id, producto_id,
            cliente_id, envio_id, float(row["Amount"]),
            int(row["Qty"]), float(row["ticket_promedio"])
        ))

    columnas_fact = [
        "order_id",
        "tiempo_id",
        "producto_id",
        "cliente_id",
        "envio_id",
        "amount",
        "qty",
        "ticket_promedio"
    ]
    df_fact_ventas = pd.DataFrame(batch_fact, columns=columnas_fact)
    insert_multi(df_fact_ventas, "fact_ventas")

    # cur.executemany("""
    #     INSERT INTO fact_ventas
    #         (order_id,tiempo_id,producto_id,cliente_id,
    #          envio_id,amount,qty,ticket_promedio)
    #     VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    #     ON CONFLICT DO NOTHING
    # """, batch_fact)
    # conn.commit()
    # log(f"CARGAR → fact_ventas: {len(batch_fact):,} insertadas | {omitidos} omitidas")

    # ── Conteo final ──────────────────────────────────────────
    conteos = {}
    for tabla in ["dim_tiempo","dim_producto","dim_cliente","dim_envio","fact_ventas"]:
        cur.execute(f"SELECT COUNT(*) FROM {tabla}")
        conteos[tabla] = cur.fetchone()[0]
    conteos["insertados_fact"] = len(batch_fact)
    conteos["omitidos_fact"]   = omitidos
    cur.close()
    return conteos


# ============================================================
# EJECUCIÓN PRINCIPAL
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("PIPELINE ETL — RetailCo")
    print("=" * 60)

    df_crudo  = extraer(bucket_name="airflow-245039720023-us-east-2-an", file_key="amazon_sale_report.csv", conn_id="s3_conn_logs")
    df_limpio = transformar(df_crudo)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        log("Conexión a PostgreSQL establecida")
    except Exception as e:
        log(f"ERROR de conexión: {e}")
        sys.exit(1)

    conteos = cargar(df_limpio, conn)
    conn.close()

    print("\n" + "=" * 60)
    print("RESUMEN FINAL")
    print("=" * 60)
    log(f"Entrantes:  {len(df_crudo):,}")
    log(f"Limpios:    {len(df_limpio):,}")
    log(f"Insertados: {conteos['insertados_fact']:,}")
    log(f"Omitidos:   {conteos['omitidos_fact']:,}")
    print()
    for tabla, count in conteos.items():
        if tabla not in ["insertados_fact", "omitidos_fact"]:
            print(f"  {tabla:20s}: {count:,}")
    log("Pipeline completado.")