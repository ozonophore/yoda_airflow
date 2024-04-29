import csv
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import dag
from airflow.operators.python import get_current_context

sys.path.append('/opt/airflow')

from dags.price.price import extract_prices, load_price, transform_prices

default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=60),
    'work_dir': 'data/prices/',
    'conn_id': 'database',
    'price_conn_id': 'pricevaru'
}


@dag(
    dag_id="prices",
    start_date=datetime(2024, 4, 27),
    schedule="0 6,12,18 * * *",
    max_active_runs=1,
    tags=['prices'],
    catchup=False,
    default_args=default_args,
)
def prices():

    @task
    def extract_data() -> str:
        id = get_current_context()["dag_run"].id
        work_dir = f"{default_args['work_dir']}/{datetime.today().strftime('%Y-%m-%d')}"
        if not os.path.exists(work_dir):
            os.makedirs(work_dir)
        file = f"{work_dir}/data_{id}.data"
        conn = Connection.get_connection_from_secrets(default_args["price_conn_id"])
        extract_prices(id=id, file_name=file, token=conn.password)
        return file

    @task
    def load_data(file: str):
        id = get_current_context()["dag_run"].id
        conn = Connection.get_connection_from_secrets(default_args["conn_id"])
        host = conn.host
        user = conn.login
        password = conn.password
        port = conn.port
        schema = conn.schema
        logging.info(f"connection: {host} schema: {schema}")
        load_price(id, file, database=schema, user=user, password=password, host=host, port=port)

    @task
    def transform_data(file: str) -> str:
        conn = Connection.get_connection_from_secrets(default_args["conn_id"])
        host = conn.host
        user = conn.login
        password = conn.password
        port = conn.port
        schema = conn.schema
        logging.info(f"connection: {host} schema: {schema}")
        return transform_prices(file_name=file, database=schema, user=user, password=password, host=host, port=port)

    load_data(transform_data(extract_data()))

prices()
