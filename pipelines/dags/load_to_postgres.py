from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import os
import sys


BUCKET     = os.environ["sirius-logs-riwi"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")

PG_CONN = {
    "host":     os.environ["PG_HOST"],
    "port":     int(os.environ.get("PG_PORT")),
    "dbname":   os.environ["PG_DBNAME"],
    "user":     os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
}

# Mapeo: nombre → (prefix en S3, tabla destino)
FORMATOS = {
    "fhv":    ("staging/fhv/",    "raw.fhv"),
    "hvfhs":  ("staging/hvfhs/",  "raw.hvfhs"),
    "green":  ("staging/green/",  "raw.green"),
    "yellow": ("staging/yellow/", "raw.yellow"),
}

LOADER_PATH = "/opt/airflow/dags/current/pipelines/scripts"  

DBT_CMD = "/home/airflow/.local/bin/dbt run"
DBT_BASE = (
    "--profiles-dir /opt/airflow/.dbt "
    "--project-dir /opt/airflow/dags/current/pipelines/data_transformation "
    "--target warehouse "
    "--target-path /tmp/dbt_target "
    "--log-path /tmp/dbt_logs "
    "2>&1"
)



def _cargar_formato(formato: str):
    sys.path.insert(0, LOADER_PATH)
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

    cargar_fhv = PythonOperator(
        task_id="load_fhv",
        python_callable=_cargar_formato,
        op_args=["fhv"],
    )
    dbt_fhv = BashOperator(
        task_id="model_fhv",
        bash_command=f"{DBT_CMD} --select warehouse.fhv {DBT_BASE}",
    )


    cargar_hvfhs = PythonOperator(
        task_id="load_hvfhs",
        python_callable=_cargar_formato,
        op_args=["hvfhs"],
    )
    dbt_hvfhs = BashOperator(
        task_id="model_hvfhs",
        bash_command=f"{DBT_CMD} --select warehouse.hvfhs {DBT_BASE}",
    )


    cargar_green = PythonOperator(
        task_id="load_green",
        python_callable=_cargar_formato,
        op_args=["green"],
    )
    dbt_green = BashOperator(
        task_id="model_green",
        bash_command=f"{DBT_CMD} --select warehouse.green {DBT_BASE}",
    )


    cargar_yellow = PythonOperator(
        task_id="load_yellow",
        python_callable=_cargar_formato,
        op_args=["yellow"],
    )
    dbt_yellow = BashOperator(
        task_id="model_yellow",
        bash_command=f"{DBT_CMD} --select warehouse.yellow {DBT_BASE}",
    )

    # ── Secuencia ─────────────────────────────────────────────────────────────
    (
        inicio
        >> cargar_fhv   >> dbt_fhv
        >> cargar_hvfhs >> dbt_hvfhs
        >> cargar_green >> dbt_green
        >> cargar_yellow >> dbt_yellow
    )