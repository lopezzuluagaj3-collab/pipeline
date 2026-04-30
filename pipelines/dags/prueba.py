from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import time


def simple_task(task_name):
    print(f"Iniciando {task_name}")
    time.sleep(5)
    print(f"Finalizando {task_name}")


with DAG(
    dag_id='simple_cluster_test',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test'],
) as dag:

    start = EmptyOperator(task_id='start')

    t1 = PythonOperator(
        task_id='task_1',
        python_callable=simple_task,
        op_kwargs={'task_name': 'task_1'}
    )

    t2 = PythonOperator(
        task_id='task_2',
        python_callable=simple_task,
        op_kwargs={'task_name': 'task_2'}
    )

    t3 = PythonOperator(
        task_id='task_3',
        python_callable=simple_task,
        op_kwargs={'task_name': 'task_3'}
    )

    end = EmptyOperator(task_id='end')

    # Estructura simple con paralelismo
    start >> [t1, t2, t3] >> end
