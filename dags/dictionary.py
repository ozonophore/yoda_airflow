import csv
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


default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=60),
    'work_dir': 'data/',
    'conn_id': 'database',
    'offset_days': 45,
}

@dag(
    dag_id="dictionary",
    start_date=datetime(2024, 1, 10),
    schedule=None,
    max_active_runs=1,
    tags=['dictionary'],
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": 30,
    },
)
def dictionary() -> None:
    r"""
    Загрузка справочников
    """

    @task
    def onec_barcodes_extract(work_dir: str) -> str:
        r"""
        Загрузка справочника штрихкодов
        """
        conn = Connection.get_connection_from_secrets(f"integration")
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
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).copy_expert(
            'COPY dl.tmp_orders_wb(date, ' +
            'owner_code, ' +
            'last_change_date, ' +
            'supplier_article, ' +
            'tech_size, ' +
            'barcode, ' +
            'total_price, ' +
            'discount_percent, ' +
            'warehouse_name, ' +
            'oblast, ' +
            'income_id, ' +
            'odid, ' +
            'subject, ' +
            'category, ' +
            'brand, ' +
            'is_cancel, ' +
            'cancel_dt, ' +
            'g_number, ' +
            'sticker, ' +
            'srid, ' +
            'order_type, ' +
            'nm_id, ' +
            'spp, ' +
            'finished_price, ' +
            'price_with_disc, ' +
            'country_name, ' +
            'oblast_okrug_name, ' +
            'region_name, '
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
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).copy_expert(
            'COPY dl.tmp_orders_wb(date, ' +
            'owner_code, ' +
            'last_change_date, ' +
            'supplier_article, ' +
            'tech_size, ' +
            'barcode, ' +
            'total_price, ' +
            'discount_percent, ' +
            'warehouse_name, ' +
            'oblast, ' +
            'income_id, ' +
            'odid, ' +
            'subject, ' +
            'category, ' +
            'brand, ' +
            'is_cancel, ' +
            'cancel_dt, ' +
            'g_number, ' +
            'sticker, ' +
            'srid, ' +
            'order_type, ' +
            'nm_id, ' +
            'spp, ' +
            'finished_price, ' +
            'price_with_disc, ' +
            'country_name, ' +
            'oblast_okrug_name, ' +
            'region_name, '
            'transaction_id) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')',
            f'{fileName}')

    @task()
    def apply_dictionary() -> None:
        pass

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

    onec_barcodes_extract_task = onec_barcodes_extract(work_dir=work_dir)
    onec_items_extract_task = onec_items_extract(work_dir=work_dir)

    [load_onec_barcodes(onec_barcodes_extract_task), load_onec_items(onec_items_extract_task)] >> apply_dictionary()

dictionary()