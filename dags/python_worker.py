from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.models import Variable
import os
# namespace = 

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 1),
    "retries": 0,
}

with DAG(
    dag_id="user_simulator_sleep_demo",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Demo: Run user-simulator image sleeping in K8s pod",
    tags=["kubernetes", "demo", "user-simulator"],
) as dag:

    start = EmptyOperator(task_id="start")

    user_sim_sleep = KubernetesPodOperator(
        task_id='user_simulator_sleep',
        namespace=Variable.get("NAMESPACE"),
        image="awesomeplant/user_simulator:latest",
        cmds=["sleep", "9999999"],         # Run sleep, not the Python script
        name="user-simulator-demo",
        get_logs=True,
        is_delete_operator_pod=True,
        env_vars={
            "KAFKA_BROKER": os.environ.get("KAFKA_BROKER"),
            "KAFKA_TOPIC": os.environ.get("KAFKA_TOPIC"),
            # Add other env vars as needed for debugging
        },
    )

    end = EmptyOperator(task_id="end")

    start >> user_sim_sleep >> end
