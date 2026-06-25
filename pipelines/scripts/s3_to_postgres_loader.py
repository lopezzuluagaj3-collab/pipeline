import io
import logging
import boto3
import pyarrow.parquet as pq
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def listar_archivos_parquet(s3_client, bucket: str, prefix: str) -> list[str]:
    """Lista recursivamente todos los .parquet dentro de un prefix, sin importar subcarpetas."""
    paginator = s3_client.get_paginator("list_objects_v2")
    archivos = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                archivos.append(obj["Key"])
    logger.info(f"Encontrados {len(archivos)} archivos en s3://{bucket}/{prefix}")
    return archivos


def cargar_archivo(s3_client, bucket: str, key: str, conn, tabla: str) -> int:
    """Lee un archivo .parquet desde S3 y lo carga a Postgres via COPY respetando las columnas."""
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    tabla_arrow = pq.read_table(io.BytesIO(obj["Body"].read()))
    df = tabla_arrow.to_pandas()

    columnas = [col.lower() for col in df.columns]
    df.columns = columnas 
    str_columnas = ", ".join(columnas)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {tabla} ({str_columnas}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
            buffer
        )
    conn.commit()
    return len(df)

def cargar_formato(
    bucket: str,
    prefix: str,
    tabla: str,
    pg_conn_params: dict,
    aws_region: str = "us-east-1",
):
    """
    Carga todos los archivos .parquet de un prefix S3 a una tabla Postgres.

    Args:
        bucket:          Nombre del bucket S3 (cuenta A).
        prefix:          Prefijo del formato, ej: 'staging/formato_1/'.
        tabla:           Tabla destino en Postgres, ej: 'raw.formato_1'.
        pg_conn_params:  Dict con host, port, dbname, user, password.
        aws_region:      Región del bucket S3.
    """
    s3 = boto3.client("s3", region_name=aws_region)
    conn = psycopg2.connect(**pg_conn_params)

    try:
        archivos = listar_archivos_parquet(s3, bucket, prefix)

        if not archivos:
            logger.warning(f"No se encontraron archivos en s3://{bucket}/{prefix}")
            return

        total_filas = 0
        for i, key in enumerate(archivos, start=1):
            logger.info(f"[{i}/{len(archivos)}] Cargando {key} → {tabla}")
            filas = cargar_archivo(s3, bucket, key, conn, tabla)
            total_filas += filas
            logger.info(f"  ✓ {filas:,} filas cargadas (acumulado: {total_filas:,})")

        logger.info(f"Carga completa: {total_filas:,} filas totales en {tabla}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error cargando {tabla}: {e}")
        raise
    finally:
        conn.close()