from airflow.decorators import dag
from datetime import datetime
from tasks.example_dag.task1 import get_server_info


@dag(
    dag_id="example_hello_world",
    schedule=None,
    catchup=False,
    tags=["example"],
)
def example_dag():

    get_server_info()

dag_instance = example_dag()