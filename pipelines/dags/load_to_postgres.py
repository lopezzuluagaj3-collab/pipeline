from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from datetime import datetime, timezone
from urllib.parse import urlparse
import os
import sys

# ─── Estáticos (no dependen del entorno) ──────────────────────────────────────
LOADER_PATH = "/opt/airflow/dags/current/pipelines/scripts"

FORMATOS = {
    "fhv":    ("tlc/staging/fhv/",    "staging.fhv"),
    "hvfhs":  ("tlc/staging/fhvhv/",  "staging.hvfhs"),
    "green":  ("tlc/staging/green/",  "staging.green"),
    "yellow": ("tlc/staging/yellow/", "staging.yellow"),
}
# ──────────────────────────────────────────────────────────────────────────────

DBT_CMD  = "/home/airflow/.local/bin/dbt run"
DBT_BASE = (
    "--profiles-dir /opt/airflow/.dbt "
    "--project-dir /opt/airflow/dags/current/pipelines/data_transformation "
    "--target warehouse "
    "--target-path /tmp/dbt_target "
    "--log-path /tmp/dbt_logs "
    "2>&1"
)

def _get_config():
    from urllib.parse import urlparse
    conn_str = os.environ["AIRFLOW_CONN_MY_POSTGRES_DB"]
    parsed = urlparse(conn_str)
    return {
        "bucket":     os.environ["S3_BUCKET"],
        "aws_region": os.environ.get("AWS_DEFAULT_REGION", "us-east-2"),
        "pg_conn": {
            "host":     parsed.hostname,
            "port":     parsed.port or 5432,
            "dbname":   parsed.path.lstrip("/"),
            "user":     parsed.username,
            "password": parsed.password,
        }
    }
def _cargar_formato(formato: str):
    config = _get_config()
    sys.path.insert(0, LOADER_PATH)
    from s3_to_postgres_loader import cargar_formato
    config = _get_config()
    prefix, tabla = FORMATOS[formato]
    cargar_formato(
        bucket=config["bucket"],
        prefix=prefix,
        tabla=tabla,
        pg_conn_params=config["pg_conn"],
        aws_region=config["aws_region"],
    )



with DAG(
    dag_id="load_postgres",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ingesta", "postgres", "s3"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    cargar_fhv = PythonOperator(
        task_id="load_fhv",
        python_callable=_cargar_formato,
        op_args=["fhv"],
    )

    cargar_hvfhs = PythonOperator(
        task_id="load_hvfhs",
        python_callable=_cargar_formato,
        op_args=["hvfhs"],
    )

    cargar_green = PythonOperator(
        task_id="load_green",
        python_callable=_cargar_formato,
        op_args=["green"],
    )

    cargar_yellow = PythonOperator(
        task_id="load_yellow",
        python_callable=_cargar_formato,
        op_args=["yellow"],
    )


    (
        inicio
        >> cargar_fhv
        >> cargar_hvfhs
        >> cargar_green
        >> cargar_yellow
    )
