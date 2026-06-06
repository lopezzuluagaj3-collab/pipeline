from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta, timezone
import boto3

MODELO         = 'stg_fhv'          # cambia por tipo
BUCKET         = 'sirius-logs-riwi'
STAGING_PREFIX = 'tlc/staging/fhv'  # cambia por tipo

def decidir(anio, mes, **context):
    s3 = boto3.client('s3', region_name='us-east-2')
    prefix = f'{STAGING_PREFIX}/anio={anio}/mes={mes:02d}/'
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
    if response.get('KeyCount', 0) > 0:
        print(f'[SKIP] {prefix} ya existe.')
        return 'skip'
    print(f'[RUN] {prefix} no existe.')
    return 'dbt_run'

with DAG(
    dag_id='fhv_staging_pipeline',  # cambia por tipo
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    params={'anio': 2015, 'mes': 1},
    tags=['fhv', 'staging', 'dbt'],
) as dag:

    branch = BranchPythonOperator(
        task_id='check_existe_s3',
        python_callable=decidir,
        op_kwargs={
            'anio': '{{ params.anio | int }}',
            'mes':  '{{ params.mes | int }}',
        },
    )

    skip = EmptyOperator(task_id='skip')

    run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            '/home/airflow/.local/bin/dbt run '
            '--select stg_fhv '  # cambia por tipo
            '--vars \'{"anio": {{ params.anio }}, "mes": {{ params.mes }}}\' '
            '--profiles-dir /opt/airflow/.dbt '
            '--project-dir /opt/airflow/dags/current/pipelines/data_transformation '
            '--target-path /tmp/dbt_target '
            '--log-path /tmp/dbt_logs '
            '2>&1'
        ),
        execution_timeout=timedelta(hours=2),
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    branch >> [skip, run]
