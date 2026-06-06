from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import boto3
import os

SPARK_INSTANCE_IDS = [
    "i-007a25c978bd7a66a",  # SVR-MASTER-SPARK
    "i-0093c9e4d7562951c",  # SVR-WORKER-SPARK-2
    "i-060d29e6d4b57e3f6",  # SVR-WORKER-SPARK-3
    "i-0809120f3580935da",  # SVR-WORKER-SPARK-4
    "i-0e1d641431a979cd3",  # SVR-WORKER-SPARK-5
    "i-0c067c808e429a7a7",  # SVR-WORKER-SPARK-6
    "i-0ce93d4c5c795981c",  # SVR-WORKER-SPARK-7
]

# ─── HELPER ───────────────────────────────────────────────────────────────────

def get_ec2_client():
    # Las variables se leen aqui, no en el modulo
    spark_role_arn = os.environ["SPARK_ROLE_ARN"]
    spark_region   = os.environ["SPARK_REGION"]

    sts = boto3.client("sts")
    creds = sts.assume_role(
        RoleArn=spark_role_arn,
        RoleSessionName="airflow-spark-session"
    )["Credentials"]
    return boto3.client(
        "ec2",
        region_name=spark_region,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"]
    )

# ─── TAREAS ───────────────────────────────────────────────────────────────────

def start_instances():
    ec2 = get_ec2_client()
    ec2.start_instances(InstanceIds=SPARK_INSTANCE_IDS)
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(
        InstanceIds=SPARK_INSTANCE_IDS,
        WaiterConfig={"Delay": 15, "MaxAttempts": 40}
    )
    print("Todas las instancias están running.")


def stop_instances():
    ec2 = get_ec2_client()
    ec2.stop_instances(InstanceIds=SPARK_INSTANCE_IDS)
    print("Todas las instancias apagadas.")

# ─── DAGs ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="spark_start",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "control"],
) as dag_start:

    PythonOperator(
        task_id="start_spark_instances",
        python_callable=start_instances,
    )


with DAG(
    dag_id="spark_stop",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "control"],
) as dag_stop:

    PythonOperator(
        task_id="stop_spark_instances",
        python_callable=stop_instances,
    )
