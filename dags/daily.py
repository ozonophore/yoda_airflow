from datetime import datetime, date

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import BranchSQLOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pendulum import duration

with DAG(
        dag_id="daily_report",
        description="Формирование ежедневных одчетов, остатков, дефектур.",
        schedule="30 23 * * *",
        start_date=datetime(2023, 1, 1, 13),
        catchup=False,
        tags=['report'],
        default_args={
            "retries": 3,
            "retry_delay": duration(seconds=30),
        },
        params={
            "report_date": Param(date.today().strftime("%Y-%m-%d"), title="Дата", format="date", type="string", description="Дата формирования отчёта"),
            "telegram_sender": Param("cosmobeautybotbot", title="Отправитель", type="string", description="Отправитель сообщения в телеграмм"),
        },
) as dag:

    daily_stock = PostgresOperator(
        task_id="daily_stock",
        postgres_conn_id="database",
        sql="call dl.calc_stock_daily_by_day(to_date('{{ params.report_date }}', 'YYYY-MM-DD'))",
        parameters={
            "report_date": dag.params["report_date"]
        },
        autocommit=True
    )

    stock_def = PostgresOperator(
        task_id="stock_def",
        postgres_conn_id="database",
        sql="call dl.calc_stock_def_by_day(to_date('{{ params.report_date }}', 'YYYY-MM-DD'))",
        parameters={
            "report_date": dag.params["report_date"]
        },
        autocommit=True
    )

    sales_stock_by_day = PostgresOperator(
        task_id="sales_stock_by_day",
        postgres_conn_id="database",
        sql="call dl.calc_sales_stock_by_day(to_date('{{ params.report_date }}', 'YYYY-MM-DD'))",
        parameters={
            "report_date": dag.params["report_date"]
        },
        autocommit=True
    )

    cluster_def = PostgresOperator(
        task_id="cluster_def",
        postgres_conn_id="database",
        sql="call dl.calc_stock_cluster_def_by_day(to_date('{{ params.report_date }}', 'YYYY-MM-DD'))",
        parameters={
            "report_date": dag.params["report_date"]
        },
        autocommit=True
    )

    cluster_report = PostgresOperator(
        task_id="cluster_report",
        postgres_conn_id="database",
        sql="call dl.calc_report_by_cluster(to_date('{{ params.report_date }}', 'YYYY-MM-DD'))",
        parameters={
            "report_date": dag.params["report_date"]
        },
        autocommit=True
    )

    item_def = PostgresOperator(
        task_id="item_def",
        postgres_conn_id="database",
        sql="call dl.calc_stock_item_def_by_day(to_date('{{ params.report_date }}', 'YYYY-MM-DD'))",
        parameters={
            "report_date": dag.params["report_date"]
        },
        autocommit=True
    )

    item_report = PostgresOperator(
        task_id="item_report",
        postgres_conn_id="database",
        sql="call dl.calc_report_by_item(to_date('{{ params.report_date }}', 'YYYY-MM-DD'))",
        parameters={
            "report_date": dag.params["report_date"]
        },
        autocommit=True
    )

    product_def = PostgresOperator(
        task_id="product_def",
        postgres_conn_id="database",
        sql="call dl.calc_item_def_by_day(to_date('{{ params.report_date }}', 'YYYY-MM-DD'))",
        parameters={
            "report_date": dag.params["report_date"]
        },
        autocommit=True
    )

    product_report = PostgresOperator(
        task_id="product_report",
        postgres_conn_id="database",
        sql="call dl.calc_report_by_product(to_date('{{ params.report_date }}', 'YYYY-MM-DD'))",
        parameters={
            "report_date": dag.params["report_date"]
        },
        autocommit=True
    )

    notification = PostgresOperator(
        task_id="notification",
        postgres_conn_id="database",
        sql="insert into ml.notification (message, sender, type, is_sent) "
            "values ('{{ params.report_date }}', '{{ params.telegram_sender }}', 'report_yesterday', False)",
        parameters={
            "report_date": dag.params["report_date"],
            "telegram_sender": dag.params["telegram_sender"]
        },
        autocommit=True
    )

    daily_stock >> stock_def >> sales_stock_by_day >> notification
    daily_stock >> cluster_def >> cluster_report >> notification
    daily_stock >> item_def >> item_report >> notification
    daily_stock >> product_def >> product_report >> notification
