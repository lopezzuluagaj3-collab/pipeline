from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta, timezone
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='maestro_staging_pipeline',
    default_args=default_args,
    description='Orquestador: dispara fhv → yellow → green → hvfhv secuencialmente',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['maestro', 'staging'],
) as dag:

    # ─────────────────────────────────────────────
    # FHV
    # ─────────────────────────────────────────────
    trigger_fhv = TriggerDagRunOperator(
        task_id='trigger_fhv',
        trigger_dag_id='fhv_staging_pipeline',
        wait_for_completion=True,      # espera a que termine antes de seguir
        poke_interval=60,              # revisa el estado cada 60 segundos
        allowed_states=['success'],    # solo avanza si terminó exitoso
        failed_states=['failed'],      # falla el maestro si el hijo falla
    )

    # ─────────────────────────────────────────────
    # YELLOW
    # ─────────────────────────────────────────────
    trigger_yellow = TriggerDagRunOperator(
        task_id='trigger_yellow',
        trigger_dag_id='yellow_staging_pipeline',
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    # ─────────────────────────────────────────────
    # GREEN
    # ─────────────────────────────────────────────
    trigger_green = TriggerDagRunOperator(
        task_id='trigger_green',
        trigger_dag_id='green_staging_pipeline',
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    # ─────────────────────────────────────────────
    # HVFHV
    # ─────────────────────────────────────────────
    trigger_hvfhv = TriggerDagRunOperator(
        task_id='trigger_hvfhv',
        trigger_dag_id='hvfhv_staging_pipeline',
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    # ─────────────────────────────────────────────
    # Secuencia: fhv → yellow → green → hvfhv
    # ─────────────────────────────────────────────
    trigger_fhv >> trigger_yellow >> trigger_green >> trigger_hvfhv