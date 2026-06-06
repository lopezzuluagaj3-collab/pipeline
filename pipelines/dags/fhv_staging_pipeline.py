from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta, timezone
import boto3

MODELO         = 'stg_fhv'
BUCKET         = 'sirius-logs-riwi'
STAGING_PREFIX = 'tlc/staging/fhv'

def ya_procesado(anio, mes, **context):
    s3 = boto3.client('s3', region_name='us-east-1')
    prefix = f'{STAGING_PREFIX}/anio={anio}/mes={mes:02d}/'
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
    existe = response.get('KeyCount', 0) > 0
    if existe:
        print(f'[SKIP] {prefix} ya existe — omitiendo.')
        return False
    print(f'[RUN] {prefix} no existe — procesando.')
    return True

with DAG(
    dag_id='fhv_staging_pipeline',
    description='FHV staging — un periodo por ejecución',
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    params={'anio': 2015, 'mes': 1},
    tags=['fhv', 'staging', 'dbt'],
) as dag:

    check = ShortCircuitOperator(
        task_id='check_existe_s3',
        python_callable=ya_procesado,
        op_kwargs={
            'anio': '{{ params.anio | int }}',
            'mes':  '{{ params.mes | int }}',
        },
    )

    run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            '/home/airflow/.local/bin/dbt run '
            '--select stg_fhv '
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

    check >> run