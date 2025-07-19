from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

with DAG(
    dag_id="example_taskgroup_dummy_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "taskgroup"],
) as dag:

    # Start task
    start = EmptyOperator(task_id="start")

    # Define TaskGroup
    with TaskGroup("dummy_group", tooltip="Group of dummy tasks") as group:
        dummy_1 = EmptyOperator(task_id="dummy_1")
        dummy_2 = EmptyOperator(task_id="dummy_2")

        dummy_1 >> dummy_2

    # End task
    end = EmptyOperator(task_id="end")

    # Set dependencies: start -> group -> end
    start >> group >> end
