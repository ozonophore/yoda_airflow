import csv
import logging
import os
import shutil
import sys
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import dag
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.append('/opt/airflow')

from dags import integration
from dags.integration.barcode_price import extract_barcode_prices, load_barcode_prices

default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=60),
    'work_dir': 'data/',
    'conn_id': 'database',
}


@dag(
    dag_id="dictionary",
    start_date=datetime(2024, 3, 18),
    schedule='@daily',
    max_active_runs=1,
    tags=['dictionary'],
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": 30,
    },
)
def dictionary():
    r"""
    Загрузка справочников
    """

    @task
    def onec_barcodes_extract(work_dir: str) -> str:
        r"""
        Загрузка справочника штрихкодов
        """
        conn = Connection.get_connection_from_secrets(f"integration")
        logging.info(f"connection: {conn.host}")
        file_name = f"{work_dir}/onec_barcodes.data"
        with open(file_name, "w") as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            id = get_current_context()["dag_run"].id
            integration.extract_barcodes(id=id,
                                         writer=writer,
                                         host=conn.host,
                                         token=conn.password)
        return file_name

    @task()
    def load_onec_barcodes(fileName: str) -> None:
        hook = PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        )
        hook.run('truncate table dl.tmp_barcode')
        hook.copy_expert(
            'COPY dl.tmp_barcode(item_id, ' +
            'barcode_id, ' +
            'barcode, ' +
            'organisation_id, ' +
            'marketplace_id, ' +
            'article, ' +
            'rrc, ' +
            'transaction_id) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')',
            f'{fileName}')


    @task
    def onec_items_extract(work_dir: str) -> str:
        r"""
        Загрузка справочника товаров
        """
        conn = Connection.get_connection_from_secrets(f"integration")
        file_name = f"{work_dir}/onec_items.data"
        with open(file_name, "w") as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            id = get_current_context()["dag_run"].id
            integration.extract_items(id=id,
                                      writer=writer,
                                      host=conn.host,
                                      token=conn.password)
        return file_name

    @task()
    def load_onec_items(fileName: str) -> None:
        pgHook = PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        )
        pgHook.run('truncate table dl.tmp_item')
        pgHook.copy_expert(
            'COPY dl.tmp_item(id, ' +
            'name, ' +
            'update_at, ' +
            'transaction_id) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')',
            f'{fileName}')

    @task()
    def extract_prices(work_dir: str) -> str:
        id=get_current_context()["dag_run"].id
        conn = Connection.get_connection_from_secrets(f"integration")
        host = conn.host
        token = conn.password
        file_name = f"{work_dir}/onec_prices.data"
        with open(file_name, "w") as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            extract_barcode_prices(id=id, writer=writer, host=host, token=token)
        return file_name

    @task()
    def load_prices(file: str) -> None:
        conn = Connection.get_connection_from_secrets(default_args["conn_id"])
        database = conn.schema
        user = conn.login
        password = conn.password
        host = conn.host
        port = conn.port
        load_barcode_prices(fileName=file, database=database, user=user, password=password, host=host, port=port)

    @task()
    def apply_dictionary() -> None:
        logging.info(f"Apply data id: {get_current_context()['dag_run'].id}")
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(
            f"call dl.apply_dictionary()")

    @task()
    def init_parameters() -> dict:
        curr_date = datetime.now()
        workDir = f"{default_args['work_dir']}dictionary_{curr_date.strftime('%Y%m%d')}"
        if os.path.exists(workDir):
            shutil.rmtree(workDir)
        os.mkdir(workDir)
        result = dict()
        result["workDir"] = workDir
        return result

    init_params = init_parameters()
    work_dir = init_params["workDir"]

    [load_onec_barcodes(onec_barcodes_extract(work_dir=work_dir)),
    load_onec_items(onec_items_extract(work_dir=work_dir)),
    load_prices(extract_prices(work_dir=work_dir))] >> apply_dictionary()


dictionary()
