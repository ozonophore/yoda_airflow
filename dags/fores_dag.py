from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import dag
from airflow.operators.python import PythonOperator
from pendulum import duration


@dag(
    dag_id="retry_dag",
    start_date=datetime(2023, 4, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(seconds=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": duration(hours=2),
    },)
def retry_dag():

    def task_1():
        return {"table": "Hello World!"}

    def task_2(**context):
        print("# table = ", context)


    t1 = PythonOperator(
        task_id="task_1",
        python_callable=task_1,
        retries=2,
    )

    t2 = PythonOperator(
        task_id="task_2",
        python_callable=task_2,
        retries=2,

    )

    @task(task_id="task_3", retries=2)
    def task_3():
        print("# task_3")

    t3 = task_3()

    t1 >> t2 >> t3

retry_dag()