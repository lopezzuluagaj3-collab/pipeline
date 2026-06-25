from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import sys
import os


BUCKET     = "sirius-logs-riwi"
AWS_REGION = "us-east-2"

PG_CONN = {
    "host":     "tu-postgres-host",
    "port":     5432,
    "dbname":   "tu-db",
    "user":     "tu-usuario",
    "password": "tu-password",
}

# Mapeo: nombre → (prefix en S3, tabla destino en Postgres)
FORMATOS = {
    "fhv":    ("tlc/staging/fhv/",    "staging.fhv"),
    "hvfhs":  ("tlc/staging/hvfhs/",  "staging.hvfhs"),
    "green":  ("tlc/staging/green/",  "staging.green"),
    "yellow": ("tlc/staging/yellow/", "staging.yellow"),
}

# Argumentos comunes para todos los comandos dbt
DBT_BASE = (
    "--profiles-dir /opt/airflow/.dbt "
    "--project-dir /opt/airflow/dags/current/pipelines/data_transformation "
    "--target warehouse "
    "--target-path /tmp/dbt_target "
    "--log-path /tmp/dbt_logs "
    "2>&1"
)

LOADER_PATH = "/opt/airflow/dags/current/pipelines/scripts/s3_to_postgres_loader.py"


def _cargar_formato(formato: str):
    sys.path.insert(0, os.path.dirname(LOADER_PATH))
    from s3_to_postgres_loader import cargar_formato
    prefix, tabla = FORMATOS[formato]
    cargar_formato(
        bucket=BUCKET,
        prefix=prefix,
        tabla=tabla,
        pg_conn_params=PG_CONN,
        aws_region=AWS_REGION,
    )


with DAG(
    dag_id="load_postgres",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ingesta", "postgres", "s3"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")
    fin    = EmptyOperator(task_id="fin")

    # ── FHV ──────────────────────────────────────────────────────────────────
    cargar_fhv = PythonOperator(
        task_id="load_fhv",
        python_callable=_cargar_formato,
        op_args=["fhv"],
    )
    dbt_fhv = BashOperator(
        task_id="model_fhv",
        bash_command=(
            f"/home/airflow/.local/bin/dbt run "
            f"--select path:models/intermediate/fhv "   # cuando esté listo
            f"{DBT_BASE}"
        ),
    )

    # ── HVFHS ────────────────────────────────────────────────────────────────
    cargar_hvfhs = PythonOperator(
        task_id="load_hvfhs",
        python_callable=_cargar_formato,
        op_args=["hvfhs"],
    )
    dbt_hvfhs = BashOperator(
        task_id="model_hvfhs",
        bash_command=(
            f"/home/airflow/.local/bin/dbt run "
            f"--select path:models/intermediate/hvfhs "  # cuando esté listo
            f"{DBT_BASE}"
        ),
    )

    # ── GREEN ─────────────────────────────────────────────────────────────────
    cargar_green = PythonOperator(
        task_id="load_green",
        python_callable=_cargar_formato,
        op_args=["green"],
    )
    # --select path:models/intermediate/green  →  corre TODOS los modelos
    # de la carpeta: dim_green_vendor, dim_green_ratecode, dim_green_payment_type,
    # dim_green_trip_type, dim_green_datetime, dim_green_location, fact_green_trips
    dbt_green = BashOperator(
        task_id="model_green",
        bash_command=(
            f"/home/airflow/.local/bin/dbt run "
            f"--select path:models/intermediate/green "
            f"{DBT_BASE}"
        ),
    )

    # ── YELLOW ────────────────────────────────────────────────────────────────
    cargar_yellow = PythonOperator(
        task_id="load_yellow",
        python_callable=_cargar_formato,
        op_args=["yellow"],
    )
    dbt_yellow = BashOperator(
        task_id="model_yellow",
        bash_command=(
            f"/home/airflow/.local/bin/dbt run "
            f"--select path:models/intermediate/yellow "  # cuando esté listo
            f"{DBT_BASE}"
        ),
    )

    # ── Dependencias ─────────────────────────────────────────────────────────
    # Se corre uno por uno para no saturar Postgres con 4 cargas masivas al tiempo
    (
        inicio
        >> cargar_fhv   >> dbt_fhv
        >> cargar_hvfhs >> dbt_hvfhs
        >> cargar_green >> dbt_green
        >> cargar_yellow >> dbt_yellow
        >> fin
    )