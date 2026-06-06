from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

def decidir(anio, mes, **context):
    import boto3
    s3 = boto3.client('s3', region_name='us-east-2')
    prefix = f'{STAGING_PREFIX}/anio={anio}/mes={mes:02d}/'
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
    if response.get('KeyCount', 0) > 0:
        print(f'[SKIP] {prefix} ya existe.')
        return 'skip'
    print(f'[RUN] {prefix} no existe.')
    return 'dbt_run'

with DAG(...) as dag:

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
        bash_command=(...),
        execution_timeout=timedelta(hours=2),
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    branch >> [skip, run]
