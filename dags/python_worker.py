from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import V1Volume, V1PersistentVolumeClaimVolumeSource, V1VolumeMount
from datetime import datetime
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import os



parquet_volume = V1Volume(
    name="parquet-data",
    persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
        claim_name="consumer-parquet-pvc"
    )
)

parquet_volume_mount = V1VolumeMount(
    name="parquet-data",
    mount_path="/data/parquet",
    read_only=False
)

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
            volumes=[parquet_volume],
            volume_mounts=[parquet_volume_mount],
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
