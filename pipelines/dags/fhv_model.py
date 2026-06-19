from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timezone

with DAG(
    dag_id='fhv_warehouse_pipeline',
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['fhv', 'warehouse', 'dbt'],
) as dag:

    run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            '/home/airflow/.local/bin/dbt run '
            '--select fhv_model '
            '--profiles-dir /opt/airflow/.dbt '
            '--project-dir /opt/airflow/dags/current/pipelines/data_transformation '
            '--target warehouse '
            '--target-path /tmp/dbt_target '
            '--log-path /tmp/dbt_logs '
            '2>&1'
        ),
    )