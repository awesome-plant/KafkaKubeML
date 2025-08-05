from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 1),
    "retries": 1,
}

with DAG(
    dag_id="streaming_ai_pipeline_kube_scalable",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Scalable AI streaming pipeline demo using K8s pods",
    tags=["kubernetes", "streaming", "ai"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Step 1: User-Simulator (mapping over env_vars)
    user_simulator = KubernetesPodOperator.partial(
        task_id='user_simulator',
        namespace="default",
        image="awesomeplant/user_simulator:latest",
        cmds=["python", "-m", "user_simulator.main"],
        name="user-simulator",
        get_logs=True,
        is_delete_operator_pod=True,
    ).expand(
        env_vars=[
            {
                "KAFKA_BROKER": os.environ.get("KAFKA_BROKER"),
                "KAFKA_TOPIC": os.environ.get("KAFKA_TOPIC"),
                "WORKER_ID": str(i),
            }
            for i in range(2)
        ]
    )

    # Step 2: Kafka Consumer (mapping over env_vars)
    kafka_consumer = KubernetesPodOperator.partial(
        task_id='stream_processor',
        namespace="default",
        image="awesome-plant/kafka-stream_processor:latest",
        cmds=["python", "-m", "stream_processor.main"],
        name="stream-processor",
        get_logs=True,
        is_delete_operator_pod=True,
    ).expand(
        env_vars=[
            {
                "KAFKA_BROKER": os.environ.get("KAFKA_BROKER"),
                "KAFKA_TOPIC":  os.environ.get("KAFKA_TOPIC"),
                "PARQUET_DIR":  os.environ.get("PARQUET_DIR"),
                "BATCH_SIZE":   os.environ.get("BATCH_SIZE"),
                "WORKER_ID": str(i),
                "PARQUET_OUTPUT_PATH": f"""/{os.environ.get("BATCH_SIZE")}/out_{i}.parquet""",
            }
            for i in range(2)
        ]
    )

    # Step 3: ML Worker (mapping over env_vars)
    ml_worker = KubernetesPodOperator.partial(
        task_id='ml_worker',
        namespace="default",
        image="awesome-plant/ml-worker:latest",
        cmds=["python", "-m", "ml_worker.main"],
        name="ml-worker",
        get_logs=True,
        is_delete_operator_pod=True,
    ).expand(
        env_vars=[
            {
                "WORKER_ID": str(i),
                "PARQUET_INPUT_PATH": f"/data/out_{i}.parquet",
            }
            for i in range(2)
        ]
    )

    # Dummy placeholders
    fastapi_server = EmptyOperator(task_id="fastapi_server")
    ui_placeholder = EmptyOperator(task_id="ui_placeholder")
    end = EmptyOperator(task_id="end")

    # DAG dependencies
    start >> user_simulator >> kafka_consumer >> ml_worker >> fastapi_server >> ui_placeholder >> end
