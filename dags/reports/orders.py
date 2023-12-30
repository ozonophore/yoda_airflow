from datetime import date, datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pendulum import duration


def get_last_transaction():
    sql_stmt = "select coalesce(max(id), 0) id from ml.transaction where status='COMPLETED'"
    pg_hook = PostgresHook(
        postgres_conn_id='database'
    )
    id = pg_hook.get_first(sql_stmt)[0]
    return id

with DAG(
        dag_id="orders_report",
        description="Формирование ежедневных отчета по остаткам за предыдущий день.",
        schedule="30 23 * * *",
        start_date=datetime(2023, 10, 4, 23),
        catchup=False,
        tags=['orders'],
        default_args={
            "retries": 3,
            "retry_delay": duration(seconds=30),
        },
        params={
            "transaction_id": Param( get_last_transaction(), title="Ид транзакции", type="integer", description="Транзакция",),
            "delete_current": Param( False, title="Удалить текущие данные", type="boolean", description="Удалить текущие данные",),
        },
) as dag:

    def check_and_delete(**context):
        delete_current = context["params"]["delete_current"]
        if delete_current:
            return "delete_current"
        else:
            return "calculate_new"


    check_and_delete_task = BranchPythonOperator(
        task_id="check_and_delete",
        python_callable=check_and_delete,
    )

    delete_current = PostgresOperator(
        task_id="delete_current",
        postgres_conn_id="database",
        sql = "delete from dl.report_order_by_day where report_date = (select (start_date - interval '1 day')::date from ml.transaction where id = {{ params.transaction_id }})",
        parameters={
            "transaction_id": dag.params["transaction_id"]
        },
        autocommit=True
    )

    calculate_new = PostgresOperator(
        task_id="calculate_new",
        postgres_conn_id="database",
        sql="call dl.calc_report_order_by_day({{ params.transaction_id }});",
        parameters={
            "transaction_id": dag.params["transaction_id"]
        },
        trigger_rule="none_failed_min_one_success",
        autocommit=True
    )

    check_and_delete_task >> [delete_current, calculate_new]
    delete_current >> calculate_new
    check_and_delete_task >> calculate_new
