import csv
import logging
import os
import shutil
import sys

import importlib_resources
import psycopg2
from airflow.models import Param, Connection, Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.append('/opt/airflow')

from dags.common.copy import copy_data_in_chunks
from dags import ozon, wb, integration, kzexp

from datetime import datetime, date, timedelta

from airflow.decorators import dag, task, task_group

default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=60),
    'work_dir': 'data/',
    'conn_id': 'database',
    'offset_days': 45,
}


def get_connection_info(source: str, owner: str) -> (str, str):
    conn = Connection.get_connection_from_secrets(f"{source.upper()}_{owner.upper()}")
    clientId = conn.login
    token = conn.password
    logging.info(f"Get connection info for {source} and {owner}")
    return (token, clientId)


@dag(
    dag_id="orders_v2",
    start_date=datetime(2024, 2, 2),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    tags=['orders'],
    params={"days": Param(45, type="integer", description="Кол-во дней для выгрузки")},
    default_args=default_args,
)
def test_dag():
    ####  WB  ###
    @task()
    def wb_extract_orders(owner: str, dateFrom: date, dateTo: date, workDir: str) -> str:
        token, clientId = get_connection_info("WB", owner)
        id = get_current_context()["dag_run"].id
        logging.info(f"Extract orders fbo for owner: {owner}, dateFrom: {dateFrom}, dateTo: {dateTo} id: {id}")
        fileName = f"{workDir}/wb_orders_{owner.lower()}_{dateFrom.strftime('%Y%m%d')}_{dateTo.strftime('%Y%m%d')}.data"
        with open(f"{fileName}", 'w') as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            wb.wb_extract_orders(id=id,
                                 writer=writer,
                                 owner=owner,
                                 token=token,
                                 dateFrom=dateFrom,
                                 dateTo=dateTo
                                 )
        return f"{fileName}"

    @task()
    def clean_table(owner: str) -> None:
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(f"delete from dl.tmp_orders_wb where owner_code = '{owner}'")

    @task()
    def wb_load_orders(fileName: str) -> None:
        sql = (
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
                'transaction_id) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')'
        )
        conn = Connection.get_connection_from_secrets(default_args["conn_id"])
        connection = psycopg2.connect(
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port
        )
        copy_data_in_chunks(fileName, sql, connection, 5000)
        # PostgresHook(
        #     postgres_conn_id=default_args["conn_id"]
        # ).copy_expert(
        #     sql,
        #     f'{fileName}')

    @task()
    def wb_extract_sale(owner: str, dateFrom: date, dateTo: date, workDir: str) -> str:
        token, clientId = get_connection_info("WB", owner)
        id = get_current_context()["dag_run"].id
        logging.info(f"Extract sales for owner: {owner}, dateFrom: {dateFrom}, dateTo: {dateTo} id: {id}")
        fileName = f"{workDir}/wb_sales_{owner.lower()}_{dateFrom.strftime('%Y%m%d')}_{dateTo.strftime('%Y%m%d')}.data"
        with open(f"{fileName}", 'w') as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            wb.wb_extract_sales(id=id,
                                writer=writer,
                                owner=owner,
                                token=token,
                                date_from=dateFrom,
                                date_to=dateTo
                                )
        return fileName

    @task()
    def wb_load_sales(fileName: str) -> None:
        sql = (
                'COPY dl.tmp_sale(date, ' +
                'owner_code, ' +
                'last_change_date, ' +
                'warehouse_name, ' +
                'country_name, ' +
                'oblast_okrug_name, ' +
                'region_name, ' +
                'supplier_article, ' +
                'barcode, ' +
                'category, ' +
                'subject, ' +
                'brand, ' +
                'tech_size, ' +
                'income_id, ' +
                'is_supply, ' +
                'is_realization, ' +
                'total_price, ' +
                'discount_percent, ' +
                'spp, ' +
                'for_pay, ' +
                'finished_price, ' +
                'price_with_disc, ' +
                'sale_id, ' +
                'sticker, ' +
                'g_number, ' +
                'odid, ' +
                'srid, ' +
                'nm_id, ' +
                'transaction_id) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')'
        )
        conn = Connection.get_connection_from_secrets(default_args["conn_id"])
        connection = psycopg2.connect(
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port
        )
        copy_data_in_chunks(fileName, sql, connection, 5000)
        # PostgresHook(
        #     postgres_conn_id=default_args["conn_id"]
        # ).copy_expert(
        #     sql,
        #     f'{fileName}')

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

    @task_group()
    def wb_tg(owner: str, dateFrom: date, dateTo: date, workDir: str):

        stock_file = wb_extract_stocks.override(task_id=f"wb_{owner.lower()}_extract_stocks")(
            owner=owner,
            workDir=workDir,
            stockDate=dateTo
        )

        wb_load_stocks.override(task_id=f"wb_{owner}_load_stocks")(
            owner=owner,
            fileName=stock_file
        )

        fileName = wb_extract_orders.override(task_id=f"wb_{owner.lower()}_extract_orders")(
            owner=owner,
            dateFrom=dateFrom,
            dateTo=dateTo,
            workDir=workDir
        )

        fileName >> clean_table(owner) >> wb_load_orders.override(task_id=f"wb_{owner.lower()}_load_orders")(fileName)

        saleFileName = wb_extract_sale.override(task_id=f"wb_{owner.lower()}_extract_sale")(
            owner=owner,
            dateFrom=dateFrom,
            dateTo=dateTo,
            workDir=workDir
        )

        wb_load_sales(saleFileName)

    #### OZON ###
    @task()
    def extract_orders_fbo(owner: str, dateFrom: date, dateTo: date, workDir: str) -> dict:
        conn = Connection.get_connection_from_secrets(f"OZON_{owner.upper()}")
        clientId = conn.login
        token = conn.password
        id = get_current_context()["dag_run"].id
        logging.info(f"Extract orders fbo for owner: {owner}, dateFrom: {dateFrom}, dateTo: {dateTo} id: {id}")
        fileName = f"orders_{owner.lower()}_fbo_{dateFrom.strftime('%Y%m%d')}_{dateTo.strftime('%Y%m%d')}.data"
        with open(f"{workDir}/{fileName}", 'w') as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            result = ozon.extract_orders_fbo(
                id,
                writer=writer,
                owner=owner,
                clientId=clientId,
                token=token,
                dateFrom=dateFrom,
                dateTo=dateTo,
            )
        d = dict()
        d["fileName"] = f"{workDir}/{fileName}"
        d["skus"] = result
        return d

    @task()
    def extract_orders_fbs(owner: str, dateFrom: date, dateTo: date, workDir: str) -> dict:
        conn = Connection.get_connection_from_secrets(f"OZON_{owner.upper()}")
        clientId = conn.login
        token = conn.password
        id = get_current_context()["dag_run"].id
        logging.info(f"Extract orders fbs for owner: {owner}, dateFrom: {dateFrom}, dateTo: {dateTo} id: {id}")
        fileName = f"orders_{owner.lower()}_fbs_{dateFrom.strftime('%Y%m%d')}_{dateTo.strftime('%Y%m%d')}.data"
        with open(f"{workDir}/{fileName}", 'w') as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            result = ozon.extract_orders_fbs(
                id=id,
                writer=writer,
                owner=owner,
                clientId=clientId,
                token=token,
                dateFrom=dateFrom,
                dateTo=dateTo,
            )
        d = dict()
        d["fileName"] = f"{workDir}/{fileName}"
        d["skus"] = result
        return d

    @task()
    def clean_data(owner: str) -> None:
        PostgresHook(postgres_conn_id=default_args["conn_id"]).run(
            "delete from dl.tmp_orders_ozon where owner_code = %s", owner)

    @task()
    def load_data(owner: str, fileName: str) -> None:
        logging.info(f"Load data from file: {fileName}")
        conn = Connection.get_connection_from_secrets(default_args["conn_id"])
        connection = psycopg2.connect(
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port
        )
        sql: str = ("COPY dl.tmp_orders_ozon("
            "owner_code," +
            "order_id," +
            "order_number," +
            "posting_number," +
            "status," +
            "created_at," +
            "in_process_at," +
            "sku," +
            "name," +
            "quantity," +
            "offer_id," +
            "price," +
            "region," +
            "city," +
            "warehouse_name," +
            "warehouse_id," +
            "commission_amount," +
            "commission_percent," +
            "payout," +
            "product_id," +
            "old_price," +
            "total_discount_value," +
            "total_discount_percent," +
            "client_price,"
            "transaction_id,"
            "schema) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', HEADER FALSE, QUOTE E'\\b')")
        copy_data_in_chunks(fileName, sql, connection, 5000)
        # PostgresHook(postgres_conn_id=default_args["conn_id"]).copy_expert(
        #     sql,
        #     fileName)

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
    def ozon_extract_stocks(owner: str, skus: set, workDir: str, stock_date: date) -> str:
        r"""
        Получение остатков
            owner - код организации
            skus - список sku
            workDir - рабочая директория
            dayTo - дата
            return - имя файла с данными
        """
        id = get_current_context()["dag_run"].id
        fileName = f"{workDir}/stock_ozon_{owner.lower()}_{stock_date.strftime('%Y%m%d')}.data"
        stock_from_sales = Variable.get("stock_from_sales", default_var=False)
        logging.info(f"Stock from sales: {stock_from_sales}")
        if stock_from_sales:
            conn_name = f"OZON_{owner.upper()}"
        else:
            conn_name = f"OZON_{owner.upper()}_STAT"
        conn = Connection.get_connection_from_secrets(conn_name)
        with open(fileName, 'w') as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            if stock_from_sales:
                ozon.extract_stock_sale(id=id, stockDate=stock_date, owner=owner, skus=skus, clientId=conn.login,
                                              token=conn.password, writer=writer)
            else:
                ozon.extract_stock(id=id, stockDate=stock_date, owner=owner, skus=skus, clientId=conn.login,
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
    def extract_product_info(owner: str, skus1: set, skus2: set, skus3: set, workDir: str) -> str:
        skus = skus1.union(skus2).union(skus3)
        fileName = f"{workDir}/product_info_{owner.lower()}.data"
        id = get_current_context()["dag_run"].id
        con = Connection.get_connection_from_secrets(f"OZON_{owner.upper()}")
        clientId = con.login
        token = con.password
        logging.info(f"Extract product info for owner: {owner}, id: {id}")
        with open(fileName, 'w') as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            ozon.extract_product_info(id=id,
                                      writer=writer,
                                      owner=owner,
                                      clientId=clientId,
                                      token=token,
                                      ids=skus
                                      )
        return fileName

    @task()
    def clean_product_info(owner: str) -> None:
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(f"delete from dl.tmp_product_info_ozon where owner_code='{owner}'")

    @task()
    def load_product_info(owner: str, fileName: str) -> None:
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(f"delete from dl.tmp_product_info_ozon where owner_code = '{owner}';")
        PostgresHook(postgres_conn_id=default_args["conn_id"]).copy_expert(
            "COPY dl.tmp_product_info_ozon(" +
            "owner_code," +
            "barcode," +
            "created_at," +
            "sku," +
            "marketing_price," +
            "min_ozon_price," +
            "min_price," +
            "offer_id," +
            "old_price," +
            "premium_price," +
            "price," +
            "name," +
            "brand," +
            "category," +
            "transaction_id) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', HEADER FALSE, QUOTE E'\\b')",
            f'{fileName}')

    @task_group()
    def ozon_tg(owner: str, dateFrom: date, dateTo: date, workDir: str):
        result_fbo = extract_orders_fbo.override(task_id=f"ozon_{owner.lower()}_extract_orders_fbo")(
            owner=owner,
            dateFrom=dateFrom,
            dateTo=dateTo,
            workDir=workDir)

        skus = ozon_extract_all_sku.override(
            task_id=f"ozon_{owner.lower()}_extract_all_sku",
        )(owner=owner)

        stock_file = ozon_extract_stocks.override(
            task_id=f"ozon_{owner.lower()}_extract_stocks"
        )(owner=owner, skus=skus, workDir=workDir, stock_date=dateTo)

        ozon_load_stocks.override(task_id=f"ozon_{owner.lower()}_load_stocks")(
            owner=owner,
            fileName=stock_file
        )

        load_data.override(task_id=f"ozon_{owner.lower()}_load_data_fbo")(owner=owner,
                                                                          fileName=result_fbo[
                                                                              "fileName"])

        result_fbs = extract_orders_fbs.override(task_id=f"ozon_{owner.lower()}_extract_orders_fbs")(owner=owner,
                                                                                                     dateFrom=dateFrom,
                                                                                                     dateTo=dateTo,
                                                                                                     workDir=workDir)
        load_data.override(task_id=f"ozon_{owner.lower()}_load_data_fbs")(owner=owner,
                                                                          fileName=result_fbs[
                                                                              'fileName'])
        file_name = extract_product_info.override(
            task_id=f"{owner.lower()}_extract_product_info")(
            owner=owner,
            skus1=result_fbs["skus"],
            skus2=result_fbo["skus"],
            skus3=skus,
            workDir=workDir
        )
        load_product_info.override(
            task_id=f"ozon_{owner.lower()}_load_product_info")(
            owner=owner,
            fileName=file_name
        )

    #### OZON END ###

    ###  INTEGRATION ###

    @task
    def onec_stocks_extract(stock_date: date, work_dir: str) -> str:
        conn = Connection.get_connection_from_secrets(f"integration")
        file_name = f"{work_dir}/onec_stock_{stock_date.strftime('%Y%m%d')}.data"
        with open(file_name, "w") as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            id = get_current_context()["dag_run"].id
            integration.extract_stock_data(id=id,
                                           writer=writer,
                                           host=conn.host,
                                           token=conn.password,
                                           date=stock_date)
        return file_name

    @task()
    def onec_stocks_load(file_name: str) -> None:
        PostgresHook(postgres_conn_id=default_args["conn_id"]).copy_expert(
            "COPY bl.stock1c(transaction_id," +
            "stock_date," +
            "item_id," +
            "quantity) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', HEADER FALSE, QUOTE E'\\b')",
            f'{file_name}')

    @task_group()
    def tg_integration(date_to: datetime, work_dir: str) -> None:
        file_name = onec_stocks_extract(stock_date=date_to, work_dir=work_dir)
        onec_stocks_load(file_name=file_name)

    ###  INTEGRATION END ###

    @task()
    def init_parameters() -> dict:
        conn = Connection.get_connection_from_secrets(conn_id=default_args["conn_id"])
        logging.info(f"Database host: {conn.host}, schem: {conn.schema} user: {conn.login}")
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(
            "truncate table dl.tmp_sale; "
            "truncate table dl.tmp_orders_wb; "
            "truncate table dl.tmp_stock_ozon; "
            "truncate table dl.tmp_orders_ozon;"
            " truncate table dl.tmp_product_info_ozon; "
            "truncate table dl.tmp_stock_wb; "
            "truncate table dl.tmp_stock_ozon_stat;")
        dateTo = date.today()
        dateFrom = dateTo - timedelta(days=get_current_context()["params"]["days"])
        workDir = f"data/orders_{dateFrom.strftime('%Y%m%d')}_{dateTo.strftime('%Y%m%d')}"
        if os.path.exists(workDir):
            shutil.rmtree(workDir)
        os.mkdir(workDir)
        result = dict()
        result["workDir"] = workDir
        result["dateTo"] = dateTo
        result["dateFrom"] = dateFrom
        return result

    #### KAZAN EXPRESS ####
    # @task()
    # def init_kz(stock_date: date, work_dir: str) -> dict:
    #     workDir = f"{work_dir}/kz_{stock_date.strftime('%Y%m%d')}"
    #     if os.path.exists(workDir):
    #         shutil.rmtree(workDir)
    #     os.mkdir(workDir)
    #     result = dict()
    #     result["work_dir"] = workDir
    #     result["stock_date"] = stock_date
    #     return result

    # @task()
    # def extract_kz_stocks(owner: str, work_dir: str, stock_date: datetime.date) -> str:
    #     logging.info("Extract stocks for %s", owner)
    #     con = Connection.get_connection_from_secrets(f"KZ_{owner.upper()}")
    #     logging.info("Login: %s", con.login)
    #     logging.info("Password: %s", con.password)
    #     logging.info("Work dir: %s", work_dir)
    #     tm_file = f"/opt/airflow/{work_dir}/kz_stock_{owner.lower()}_{stock_date.strftime('%Y%m%d')}.csv"
    #     logging.info("Target file: %s", tm_file)
    #     kzexp.extract_data(target=tm_file, login=con.login, password=con.password, context=get_current_context())
    #     return tm_file

    # @task()
    # def transform_kz_stocks(source: str, owner: str, stock_date: datetime.date, work_dir: str) -> str:
    #     logging.info("Transform stocks for %s", source)
    #     file_name = f"{work_dir}/kz_stock_{owner.lower()}_{stock_date.strftime('%Y%m%d')}.data"
    #     id = get_current_context()["dag_run"].id
    #     with open(file_name, "w") as f:
    #         writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
    #         kzexp.transform_data(id=id,
    #                              source=source,
    #                              stock_date=stock_date,
    #                              writer=writer,
    #                              owner=owner
    #                              )
    #     return file_name

    # @task()
    # def kz_stocks_load(file_name: str, owner: str) -> None:
    #     kzexp.load_data(fileName=file_name, owner=owner, con_id=default_args["conn_id"])
    #
    # @task_group()
    # def kz_stock_tg(stock_date: datetime.date, workDir: str, kz_orgs: str) -> None:
    #     for kz_org in kz_orgs:
    #         extract_file = extract_kz_stocks.override(task_id=f"extract_stocks_{kz_org.lower()}")(owner=kz_org,
    #                                                                                               work_dir=workDir,
    #                                                                                               stock_date=stock_date)
    #         data_file = transform_kz_stocks.override(task_id=f"transform_stocks_{kz_org.lower()}")(source=extract_file,
    #                                                                                                owner=kz_org,
    #                                                                                                stock_date=stock_date,
    #                                                                                                work_dir=workDir)
    #         kz_stocks_load.override(task_id=f"load_stocks_{kz_org.lower()}")(file_name=data_file, owner=kz_org)

    ### APPLY ORDERS ###
    @task
    def apply_data(date_from: date) -> None:
        logging.info(f"Apply data by {date_from}")
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(
            f"call dl.apply_orders(to_date('{date_from}', 'YYYY-MM-DD'))")

    @task
    def apply_sale(date_from: date) -> None:
        logging.info(f"Apply sale by {date_from}")
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(
            f"call dl.apply_sale(to_date('{date_from}', 'YYYY-MM-DD'))")

    @task
    def apply_stock(stock_date: date) -> None:
        logging.info(f"Apply stock {stock_date}")
        PostgresHook(postgres_conn_id=default_args["conn_id"]).run(
            f"call dl.apply_stock(to_date('{stock_date}', 'YYYY-MM-DD'))")

    @task
    def apply_stock_kz(stock_date: date) -> None:
        logging.info(f"Apply stock kz {stock_date}")
        PostgresHook(postgres_conn_id=default_args["conn_id"]).run(
            f"call dl.apply_stock_kz(to_date('{stock_date}', 'YYYY-MM-DD'))")

    #######################

    initParams = init_parameters()
    dateTo = initParams["dateTo"]
    dateFrom = initParams["dateFrom"]
    workDir = initParams["workDir"]

    # kz_init = init_kz(stock_date=dateTo, work_dir=workDir)
    # kz_stock_date = kz_init["stock_date"]
    # kz_work_dir = kz_init["work_dir"]
    # kz_orgs = []

    apply_data_task = apply_data(dateFrom)

    apply_sale_task = apply_sale(dateFrom)

    apply_stock_task = apply_stock(stock_date=dateTo)

    # apply_stock_kz_task = apply_stock_kz(stock_date=dateTo)

    sql_path = importlib_resources.files(__name__).joinpath("sql/select_orgs.sql")
    with open(sql_path, "r") as file:
        sql_query = file.read()
        orgs = PostgresHook(postgres_conn_id=default_args["conn_id"]).get_records(sql_query)
        for org in orgs:
            source = org[0]
            owner = org[1]
            try:
                Connection.get_connection_from_secrets(f"OZON_{owner.upper()}")
            except Exception as e:
                logging.error(f"Error connection for owner: {owner}, msg: {e}")
                continue

            ### OZON ###
            if source == "OZON":
                ozon_tg.override(group_id=f"{source}_{owner}".lower())(
                    owner=owner,
                    dateTo=dateTo,
                    dateFrom=dateFrom,
                    workDir=workDir
                ) >> [apply_stock_task, apply_data_task, apply_sale_task]
            elif source == "WB":
                ###  WB  ###
                wb_tg.override(group_id=f"{source}_{owner}".lower())(
                    owner=owner,
                    dateFrom=dateFrom,
                    dateTo=dateTo,
                    workDir=workDir
                ) >> [apply_stock_task, apply_data_task, apply_sale_task]
            # elif source == "KZEXP":
            #     kz_orgs.append(owner)

    # kz_loaded = kz_stock_tg(stock_date=kz_stock_date, workDir=kz_work_dir, kz_orgs=kz_orgs)
    #
    # apply_stock_task >> kz_loaded >> apply_stock_kz_task

    ###  INTEGRATION  ###

    tg_integration(date_to=dateTo, work_dir=workDir) >> [apply_stock_task, apply_data_task]


test_dag()
