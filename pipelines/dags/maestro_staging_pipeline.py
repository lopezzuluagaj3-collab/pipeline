from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='maestro_staging_pipeline',
    default_args=default_args,
    description='Orquestador maestro: fhv → yellow → green → hvfhs secuencialmente',
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['maestro', 'staging'],
) as dag:

    trigger_fhv = TriggerDagRunOperator(
        task_id='trigger_fhv',
        trigger_dag_id='fhv_orquestador',
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_yellow = TriggerDagRunOperator(
        task_id='trigger_yellow',
        trigger_dag_id='yellow_orquestador',
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_green = TriggerDagRunOperator(
        task_id='trigger_green',
        trigger_dag_id='green_orquestador',
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_hvfhs = TriggerDagRunOperator(
        task_id='trigger_hvfhs',
        trigger_dag_id='hvfhs_orquestador',
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_fhv >> trigger_yellow >> trigger_green >> trigger_hvfhs