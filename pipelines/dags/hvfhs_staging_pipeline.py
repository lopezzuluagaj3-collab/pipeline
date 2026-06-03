from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta, timezone
from datetime import datetime, timedelta
import boto3


# ─────────────────────────────────────────────
# Parámetros del pipeline — tu compañero solo
# cambia MODELO y BUCKET_STAGING para su tipo
# ─────────────────────────────────────────────
MODELO          = 'stg_hvfhs'
BUCKET          = 'sirius-logs-riwi'
STAGING_PREFIX  = 'tlc/staging/hvfhs'
ANIO_INICIO     = 2019
ANIO_FIN        = 2026


def generar_periodos(anio_inicio, anio_fin):
    periodos = []
    hoy = datetime.today()
    for anio in range(anio_inicio, anio_fin + 1):
        for mes in range(1, 13):
            if datetime(anio, mes, 1) > hoy.replace(day=1):
                return periodos
            periodos.append((anio, mes))
    return periodos


def ya_procesado(anio, mes, **context):
    """
    Verifica si el prefijo ya existe en S3 staging.
    Si existe → ShortCircuit devuelve False → skip del dbt run.
    Si no existe → devuelve True → corre dbt run.
    """
    s3 = boto3.client('s3', region_name='us-east-1')
    prefix = f'{STAGING_PREFIX}/anio={anio}/mes={mes}/'

    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=prefix,
        MaxKeys=1
    )

    existe = response.get('KeyCount', 0) > 0

    if existe:
        print(f'[SKIP] {prefix} ya existe en S3 — omitiendo.')
        return False  # ShortCircuit: no corre el siguiente task

    print(f'[RUN] {prefix} no existe — procesando.')
    return True  # ShortCircuit: sí corre el siguiente task


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='hvfhs_staging_pipeline',
    default_args=default_args,
    description='HVFHS staging mes a mes con skip si ya fue procesado',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['hvfhs', 'staging', 'dbt'],
) as dag:

    periodos = generar_periodos(ANIO_INICIO, ANIO_FIN)
    tarea_anterior = None

    for anio, mes in periodos:

        # ── Task 1: verificar si ya existe en S3 ──
        check = ShortCircuitOperator(
            task_id=f'check_{anio}_{mes:02d}',
            python_callable=ya_procesado,
            op_kwargs={'anio': anio, 'mes': mes},
            ignore_downstream_trigger_rules=False,
        )

        # ── Task 2: correr dbt solo si no existe ──
        run = BashOperator(
            task_id=f'stg_hvfhs_{anio}_{mes:02d}',
            bash_command=(
                f'dbt run '
                f'--select {MODELO} '
                f'--vars \'{{"anio": {anio}, "mes": {mes}}}\' '
                f'--profiles-dir /opt/airflow/.dbt '
                f'--project-dir /opt/airflow/sirius_project '
            ),
            retries=2,
            retry_delay=timedelta(minutes=5),
        )

        # check → run secuencial
        check >> run

        # encadenar al periodo anterior
        if tarea_anterior is not None:
            tarea_anterior >> check

        tarea_anterior = run