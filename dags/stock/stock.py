import csv
import logging
import os
import shutil
import sys
from datetime import datetime, timedelta, date

sys.path.append('/opt/airflow')

from airflow.decorators import task, task_group
from airflow.models import Connection
from airflow.models.dag import dag
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags import ozon, wb

default_args = {
    'conn_id': 'database_test',
}


def stock():
    @task()
    def ozon_extract_all_sku(owner: str) -> set:
        r"""
        Получение списка всех sku
            owner - код организации
            return - список sku
        """
        conn = Connection.get_connection_from_secrets(f"OZON_{owner.upper()}")
        product_ids = ozon.get_product_ids(clientId=conn.login, token=conn.password)
        return ozon.get_sku_by_product_id(clientId=conn.login, token=conn.password, product_ids=product_ids)

    @task()
    def ozon_extract_stocks(owner: str, skus: set, workDir: str, dayTo: date) -> str:
        r"""
        Получение остатков
            owner - код организации
            skus - список sku
            workDir - рабочая директория
            dayTo - дата
            return - имя файла с данными
        """
        id = get_current_context()["dag_run"].id
        conn = Connection.get_connection_from_secrets(f"OZON_{owner.upper()}_STAT")
        fileName = f"{workDir}/stock_ozon_{owner.lower()}_{dayTo.strftime('%Y%m%d')}.data"
        with open(fileName, 'w') as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            stockDate = dayTo - timedelta(days=1)
            ozon.extract_stock(id=id, stockDate=stockDate, owner=owner, skus=skus, clientId=conn.login,
                               token=conn.password, writer=writer)
        return fileName

    @task()
    def ozon_load_stocks(owner: str, fileName: str) -> None:
        r"""
        Загрузка данных в БД
            owner - код организации
            fileName - имя файла с данными
        """
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(f"delete from dl.tmp_stock_ozon_stat where owner = '{owner}';")
        logging.info(f"Load data from file: {fileName}")
        PostgresHook(postgres_conn_id=default_args["conn_id"]).copy_expert(
            "COPY dl.tmp_stock_ozon_stat(" +
            "owner, " +
            "date, " +
            "sku, " +
            "method, " +
            "category, " +
            "currency, " +
            "base_price, " +
            "discount_price, " +
            "premium_price, " +
            "ozon_card_price, " +
            "name, " +
            "brand, " +
            "seller, " +
            "warehouse, " +
            "warehouse_region, " +
            "warehouse_id, " +
            "quantity, " +
            "transaction_id, " +
            "barcode) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', HEADER FALSE, QUOTE E'\\b')",
            f'{fileName}')

    @task()
    def wb_extract_stocks(owner: str, workDir: str, stockDate: date) -> str:
        r"""
        Получение остатков
            owner - код организации
            workDir - рабочая директория
            stockDate - дата
            return - имя файла с данными
        """
        id = get_current_context()["dag_run"].id
        token = Connection.get_connection_from_secrets(f"WB_{owner.upper()}").password
        fileName = f"{workDir}/stock_wb_{owner.lower()}_{stockDate.strftime('%Y%m%d')}.data"
        with open(fileName, 'w') as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            dateFrom = stockDate - timedelta(days=30)
            wb.extract_stock(id=id, owner=owner, writer=writer, stockDate=stockDate, dateFrom=dateFrom, token=token)
        return fileName

    @task()
    def wb_load_stocks(owner: str, fileName: str) -> None:
        r"""
                Загрузка данных в БД
                    owner - код организации
                    fileName - имя файла с данными
                """
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(f"delete from dl.tmp_stock_wb where owner_code = '{owner}';")
        logging.info(f"Load data from file: {fileName}")
        PostgresHook(postgres_conn_id=default_args["conn_id"]).copy_expert(
            "COPY dl.tmp_stock_wb(" +
            "transaction_id, " +
            "owner_code, " +
            "last_change_date, " +
            "warehouse_name, " +
            "supplier_article, " +
            "barcode, " +
            "quantity, " +
            "in_way_to_client, " +
            "in_way_from_client, " +
            "quantity_full, " +
            "category, " +
            "subject, " +
            "brand, " +
            "price, " +
            "discount, " +
            "is_supply, " +
            "is_realization, " +
            "\"nmId\", " +
            "create_at" +
            ") FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', HEADER FALSE, QUOTE E'\\b')",
            f'{fileName}')

    @task
    def apply_stock(stock_date: date) -> None:
        logging.info(f"Apply stock {stock_date}")
        PostgresHook(postgres_conn_id=default_args["conn_id"]).run(
            f"call dl.apply_stock(to_date('{stock_date}', 'YYYY-MM-DD'))")

    @task_group()
    def tg_stock(owner: str, workDir: str, stock_date: date):
        fileName = wb_extract_stocks.override(task_id=f"wb_{owner.lower()}_extract_stocks")(owner=owner,
                                                                                            workDir=workDir,
                                                                                            stockDate=stock_date)
        wb_load_stocks.override(task_id=f"wb_{owner.lower()}_load_stocks")(owner=owner,
                                                                           fileName=fileName
                                                                           )

        Connection.get_connection_from_secrets(f"OZON_{owner.upper()}_STAT")
        skus = ozon_extract_all_sku.override(
            task_id=f"ozon_{owner.lower()}_extract_all_sku")(owner=owner)
        fileName = ozon_extract_stocks.override(task_id=f"ozon_{owner.lower()}_extract_stocks")(owner=owner,
                                                                                                skus=skus,
                                                                                                workDir=workDir,
                                                                                                dayTo=stock_date)
        ozon_load_stocks.override(task_id=f"ozon_{owner.lower()}_load_stocks")(owner=owner,
                                                                               fileName=fileName
                                                                               )

    @task()
    def init_stock_parameters() -> dict:
        stockDate = datetime.now().date()
        logging.info(f"Stock date: {stockDate}")
        workDir = f"data/stocks_{stockDate.strftime('%Y%m%d')}"
        if os.path.exists(workDir):
            shutil.rmtree(workDir)
        os.mkdir(workDir)
        return {
            "stock_date": stockDate,
            "work_dir": workDir,
        }

    params = init_stock_parameters()
    stockDate = params["stock_date"]
    workDir = params["work_dir"]

    orgs = PostgresHook(postgres_conn_id=default_args["conn_id"]).get_records(
        "SELECT code FROM ml.owner WHERE is_deleted is false and code='DREAMLAB'")

    apply_stock_task = apply_stock(stock_date=stockDate)

    for org in orgs:
        owner = org[0]

        tg_stock.override(group_id=f"{owner}_stocks")(
            owner=owner,
            workDir=workDir,
            stock_date=stockDate
        ) >> apply_stock_task


stock()
