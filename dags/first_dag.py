from airflow import DAG
from datetime import timedelta, datetime

from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12)
}

@dag(
    schedule="0 0 * * *",
    start_date=datetime.today() - timedelta(days=2),
    dagrun_timeout=timedelta(minutes=60),
)
def ETL():
    # populate_table = PostgresOperator(
    #     task_id="populate_pet_table",
    #     postgres_conn_id="database",
    #     sql="select * from ml.transaction",
    # )

    def get_data():
        sql_stmt = "select id, status from ml.transaction"
        pg_hook = PostgresHook(
            postgres_conn_id="database"
        )
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(sql_stmt)
        return cursor.fetchall()
    #
    # select_data = PostgresHook(postgres_conn_id="database").exe("select * from ml.transaction")

    # populate_table = SQLExecuteQueryOperator(
    #     task_id="select_from_table",
    #     postgres_conn_id="database",
    #     sql="select * from ml.transaction",
    # )

    @task
    def print_text(data):
        print("BEGIN")

        for row in data:
            print(row['id'], " | ", row['status'])

        print("Hello World! ")

    print_text(get_data())

dag_run = ETL()
