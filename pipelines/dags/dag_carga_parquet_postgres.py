from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import sys


BUCKET = "sirius-logs-riwi"         
AWS_REGION = "us-east-2"              

PG_CONN = {
    "host":     "tu-postgres-host",    
    "port":     5432,
    "dbname":   "tu-db",              
    "user":     "tu-usuario",         
    "password": "tu-password",        
    }

# Mapeo: nombre → (prefix en S3, tabla destino)
FORMATOS = {
    "formato_1": ("staging/fhv/", "raw.formato_1"),
    "formato_2": ("staging/fhvhv/", "raw.formato_2"),
    "formato_3": ("staging/green/", "raw.formato_3"),
    "formato_4": ("staging/yellow/  ", "raw.formato_4"),
}

DBT_BASE = (
    '--profiles-dir /opt/airflow/.dbt '
    '--project-dir /opt/airflow/dags/current/pipelines/data_transformation '
    '--target warehouse '
    '--target-path /tmp/dbt_target '
    '--log-path /tmp/dbt_logs '
    '2>&1'
)

LOADER_PATH = "/opt/airflow/dags/current/pipelines/scripts/s3_to_postgres_loader.py"


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

    cargar_f1 = PythonOperator(
        task_id="load_fhv",
        python_callable=_cargar_formato,
        op_args=["formato_1"],
    )
    dbt_f1 = BashOperator(
        task_id="model_fhv",
        bash_command=f"/home/airflow/.local/bin/dbt run --select int_formato_1 {DBT_BASE}",
    )

    cargar_f2 = PythonOperator(
        task_id="load_fhvhv",
        python_callable=_cargar_formato,
        op_args=["formato_2"],
    )
    dbt_f2 = BashOperator(
        task_id="model_fhvhv",
        bash_command=f"/home/airflow/.local/bin/dbt run --select int_formato_2 {DBT_BASE}",
    )

    cargar_f3 = PythonOperator(
        task_id="load_green",
        python_callable=_cargar_formato,
        op_args=["formato_3"],
    )
    dbt_f3 = BashOperator(
        task_id="model_green",
        bash_command=f"/home/airflow/.local/bin/dbt run --select int_formato_3 {DBT_BASE}",
    )

    cargar_f4 = PythonOperator(
        task_id="load_yellow",
        python_callable=_cargar_formato,
        op_args=["formato_4"],
    )
    dbt_f4 = BashOperator(
        task_id="model_yellow",
        bash_command=f"/home/airflow/.local/bin/dbt run --select int_formato_4 {DBT_BASE}",
    )

    inicio >> cargar_f1 >> dbt_f1 >> cargar_f2 >> dbt_f2 >> cargar_f3 >> dbt_f3 >> cargar_f4 >> dbt_f4