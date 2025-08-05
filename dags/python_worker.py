from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 1),
    "retries": 0,
}

with DAG(
    dag_id="stream_processor_sleep_demo",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Demo: Run stream-processor image sleeping in K8s pod",
    tags=["kubernetes", "demo", "stream-processor"],
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup("process_data", tooltip="Processing pipeline") as process_data:
        user_simulator = KubernetesPodOperator(
            task_id='user_simulator',
            namespace=Variable.get("NAMESPACE"),
            image="awesomeplant/user_simulator:latest",
            cmds=["sleep", "9999999"], #["python", "-m", "user_simulator.main"],
            name="user-simulator",
            get_logs=True,
            is_delete_operator_pod=True,
            env_vars={
                    "KAFKA_BROKER": os.environ.get("KAFKA_BROKER"),
                    "KAFKA_TOPIC": os.environ.get("KAFKA_TOPIC")
                }
        )
        user_sim_sleep = KubernetesPodOperator(
            task_id='stream_processor_sleep',
            namespace=Variable.get("NAMESPACE"),
            # image="awesomeplant/user_simulator:latest",
            image="awesomeplant/stream_processor:latest",
            cmds=["sleep", "9999999"],         # Run sleep, not the Python script
            name="stream_processor-demo",
            get_logs=True,
            is_delete_operator_pod=True,
            env_vars={
                "KAFKA_BROKER": os.environ.get("KAFKA_BROKER"),
                "KAFKA_TOPIC":  os.environ.get("KAFKA_TOPIC"),
                "PARQUET_DIR":  os.environ.get("PARQUET_DIR"),
                "BATCH_SIZE":   os.environ.get("BATCH_SIZE")
            },
        )
        [user_simulator, user_sim_sleep]

    end = EmptyOperator(task_id="end")

    start >> process_data >> end
