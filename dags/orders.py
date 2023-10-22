import csv
import logging
import os
import uuid
from array import array
from datetime import datetime, timedelta
from time import sleep

import requests
from airflow.decorators import task
from airflow.models import Param
from airflow.models.dag import dag
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.types import NOTSET
from dateutil.utils import today

default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=60),
    'work_dir': 'data/',
    'conn_id': 'database',
    'offset_days': 45,
}


def request_repeater(method, url, **kwargs) -> requests.Response:
    attempt = 3
    while True:
        resp = requests.request(method, url, **kwargs)
        if resp.status_code == 200:
            return resp
        if attempt == 0:
            raise Exception(f"Response code: {resp.status_code} message: {resp.text}")
        logging.info(f"Attemption: {attempt} code: {resp.status_code} message: {resp.text}")
        attempt -= 1
        sleep(30)

@dag(
    start_date=datetime.today(),
    schedule_interval=NOTSET,
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=3,
    params={
        "dateTo": Param(today().strftime("%Y-%m-%d"), title="Дата до", format="date",
                                 type="string", description="Дата до",),
    },
    tags=['orders'])
def orders():
    @task
    def clean_and_init() -> None:
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(
            "truncate table dl.tmp_sale; truncate table dl.tmp_orders_wb; truncate table dl.tmp_stock_ozon; truncate table dl.tmp_orders_ozon;"
            " truncate table dl.tmp_product_info_ozon; truncate table dl.tmp_stock_wb;")

    @task
    def wb_extract_sales(owner, token) -> None:
        offset_days = default_args["offset_days"]
        dateToStr = get_current_context()['params']['dateTo']
        dateTo = datetime.strptime(dateToStr, '%Y-%m-%d')
        startdate = dateTo - timedelta(days=offset_days)
        filename = uuid.uuid4()
        csvfile = open(f'{default_args["work_dir"]}{filename}.{owner}.sale.csv', 'w')
        try:
            while True:
                resp = request_repeater("GET",f'https://statistics-api.wildberries.ru/api/v1/supplier/sales?dateFrom={startdate}&flag=0',
                                        headers={"Authorization": f"Bearer {token}"})
                if len(resp.json()) == 0:
                    break
                spamwriter = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                logging.info("Size: %s", len(resp.json()))
                for row in resp.json():
                    startdate = max(startdate, datetime.strptime(row['lastChangeDate'], '%Y-%m-%dT%H:%M:%S'))
                    if datetime.strptime(row['date'], '%Y-%m-%dT%H:%M:%S') < startdate:
                        continue  # skip old data
                    spamwriter.writerow(
                        [row['date'], owner, row['lastChangeDate'], row['warehouseName'], row['countryName'],
                         row['oblastOkrugName'], row['regionName'], row['supplierArticle'], row['barcode'],
                         row['category'], row['subject'], row['brand'], row['techSize'], row['incomeID'], row['isSupply'],
                         row['isRealization'], row['totalPrice'], row['discountPercent'], row['spp'], row['forPay'],
                         row['finishedPrice'], row['priceWithDisc'], row['saleID'], row['sticker'],
                         row['gNumber'], row['odid'], row['srid'], row['nmId']])
            csvfile.close()
            PostgresHook(
                postgres_conn_id=default_args["conn_id"]
            ).copy_expert(
                'COPY dl.tmp_sale FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')',
                f'{csvfile.name}')
        finally:
            os.remove(csvfile.name)

    def prepare_wb_date(owner, dateFrom, csvfile, token) -> datetime:
        logging.info("Date from: %s", dateFrom)
        startdate = dateFrom.strftime('%Y-%m-%dT%H:%M:%S')
        attempt = 3
        while True:
            resp = requests.get(
                f'https://statistics-api.wildberries.ru/api/v1/supplier/orders?dateFrom={startdate}&flag=0',
                headers={"Authorization": f"Bearer {token}"})
            if resp.status_code != 200 and attempt == 0:
                raise Exception('GET ORDERS {}'.format(resp.status_code))
            if resp.status_code == 200:
                break
            attempt -= 1
            sleep(30)
        size = len(resp.json())
        logging.info("Size: %s", size)
        if size == 0:
            return None
        maxDate = dateFrom
        spamwriter = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        i = 0
        allCount = 0
        for row in resp.json():
            allCount += 1
            if datetime.strptime(row['date'], '%Y-%m-%dT%H:%M:%S') < dateFrom:
                continue # skip old data
            maxDate = max(maxDate, datetime.strptime(row['lastChangeDate'], '%Y-%m-%dT%H:%M:%S'))
            spamwriter.writerow(
                [row['date'], owner, row['lastChangeDate'], row['supplierArticle'], row['techSize'],
                 row['barcode'], row['totalPrice'], row['discountPercent'], row['warehouseName'],
                 row['oblast'], row['incomeID'], row['odid'], row['subject'], row['category'],
                 row['brand'], row['isCancel'], row['cancel_dt'], row['gNumber'], row['sticker'], row['srid'],
                 row['orderType'], row['nmId']])
            i += 1
        logging.info("Inserted: %s from %s", i, allCount)
        return maxDate + timedelta(seconds=1)

    @task
    def wb_extract_orders(owner, token) -> None:
        dateToStr = get_current_context()['params']['dateTo']
        dateTo = datetime.strptime(dateToStr, '%Y-%m-%d')
        filename = uuid.uuid4()
        csvfile = open(f'{default_args["work_dir"]}{filename}.{owner}.order.wb.csv', 'w')
        try:
            offset_days = default_args["offset_days"]
            startDate = dateTo - timedelta(days=offset_days)
            while startDate is not None:
                startDate = prepare_wb_date(owner, startDate, csvfile, token)
            csvfile.close()

            PostgresHook(
                postgres_conn_id=default_args["conn_id"]
            ).copy_expert(
                'COPY dl.tmp_orders_wb FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')',
                f'{csvfile.name}')
        finally:
            os.remove(csvfile.name)

    @task
    def wb_extract_stock(owner, token) -> None:
        dateToStr = get_current_context()['params']['dateTo']
        dateTo = datetime.strptime(dateToStr, '%Y-%m-%d')
        offset_days = default_args["offset_days"]
        dateFrom = dateTo - timedelta(days=offset_days)
        dateFromStr = dateFrom.strftime('%Y-%m-%d')
        resp = request_repeater("GET", f"https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={dateFromStr}",
                            headers={"Authorization": f"Bearer {token}"})
        items = resp.json()
        if len(items) == 0:
            return None
        csvfile = open(f'{default_args["work_dir"]}{uuid.uuid4()}.{owner}.stock.wb.csv', 'w')
        try:
            spamwriter = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            for item in items:
                spamwriter.writerow(
                    [owner, item['lastChangeDate'], item['warehouseName'], item['supplierArticle'], item['barcode'],
                     item['quantity'], item['inWayToClient'], item['inWayFromClient'], item['quantityFull'],
                     item['category'], item['subject'], item['brand'], item['Price'],
                     item['Discount'], item['isSupply'], item['isRealization'], item['nmId'] ])
            csvfile.close()
            PostgresHook(postgres_conn_id=default_args["conn_id"]).copy_expert(
                'COPY dl.tmp_stock_wb FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')',
                f'{csvfile.name}')
        finally:
            os.remove(csvfile.name)

    @task
    def start() -> None:
        pass

    @task
    def ozon_extract_orders(owner, clientId, token) -> set:
        dateToStr = get_current_context()['params']['dateTo']
        dateTo = datetime.strptime(dateToStr, '%Y-%m-%d')
        offset_days = default_args["offset_days"]
        sinceDate = (dateTo - timedelta(days=offset_days)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        toDate = dateTo.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        logging.info(f"From date: {sinceDate} to {toDate} ")
        headers = {
            "Client-Id": clientId,
            "Api-Key": token,
            "Content-Type": "application/json",
        }
        offset = 0
        pageSize = 1000
        csvfile = open(f'{default_args["work_dir"]}{uuid.uuid4()}.{owner.lower()}.order.ozon.csv', 'w')
        skus = set()
        spamwriter = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        maxDate = datetime.strptime(sinceDate, '%Y-%m-%dT%H:%M:%S.%fZ')
        try:
            while True:
                if offset > 20000:
                    offset = 0
                    sinceDate = (maxDate + timedelta(milliseconds=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    logging.info("Since date: %s", sinceDate)
                resp = request_repeater('POST', "https://api-seller.ozon.ru/v2/posting/fbo/list", headers=headers, json={
                    "dir": "ASC",
                    "filter": {
                        "since": sinceDate,
                        "status": "",
                        "to": toDate
                    },
                    "limit": pageSize,
                    "offset": offset,
                    "translit": True,
                    "with": {
                        "analytics_data": True,
                        "financial_data": True
                    }
                }
                                     )
                items = resp.json()['result']
                size = len(items)
                if size == 0:
                    break
                product_count = 0
                for item in items:
                    maxDate = max(maxDate, datetime.strptime(item['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ'))
                    analytics_data = item['analytics_data']
                    financial_dict = dict()
                    if item['financial_data'] is not None:
                        financial_data = item['financial_data']['products']
                        financial_dict = dict()
                        for fd in financial_data:
                            financial_dict[fd['product_id']] = fd
                    products = item['products']
                    for index, product in enumerate(products, start=0):
                        fd = financial_dict[product['sku']]
                        skus.add(product['sku'])
                        spamwriter.writerow(
                            [owner,
                             item['order_id'], item['order_number'], item['posting_number'], item['status'],
                             item['created_at'], item['in_process_at'], product['sku'], product['name'],
                             product['quantity'],
                             product['offer_id'],
                             product['price'], analytics_data['region'], analytics_data['city'],
                             analytics_data['warehouse_name'],
                             analytics_data['warehouse_id'], fd['commission_amount'], fd['commission_percent'],
                             fd['payout'], fd['product_id'], fd['old_price'], fd['total_discount_value'],
                             fd['total_discount_percent'], fd['client_price']
                             ])
                        product_count += 1

                if size < pageSize:
                    break
                offset += pageSize
                logging.info("Offset: %s", offset)
            csvfile.close()
            PostgresHook(postgres_conn_id=default_args["conn_id"]).copy_expert(
                'COPY dl.tmp_orders_ozon FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')',
                f'{csvfile.name}')
            logging.info("Inserted: %s", product_count)
        finally:
            os.remove(csvfile.name)
        return skus

    @task
    def ozon_extract_stock(owner, clientId, token) -> None:
        headers = {
            "Client-Id": clientId,
            "Api-Key": token,
            "Content-Type": "application/json",
        }
        offset = 0
        pageSize = 1000
        skus = set()
        csvfile = open(f'{default_args["work_dir"]}{uuid.uuid4()}.{owner.lower()}.stock.ozon.csv', 'w')
        while True:
            resp = request_repeater('POST', "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=headers
                                 , json={
                    "warehouse_type": "ALL",
                    "limit": pageSize,
                    "offset": offset
                }
                                 )
            items = resp.json()['result']['rows']
            size = len(items)
            logging.info("Size: %s", size)
            spamwriter = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            for row in items:
                skus.add(row['sku'])
                spamwriter.writerow(
                    [row['sku'], row['item_code'], row['item_name'], row['free_to_sell_amount'], row['promised_amount'],
                     row['reserved_amount'], row['warehouse_name'], owner]
                )

            if size < pageSize:
                break
            offset += pageSize
        csvfile.close()
        PostgresHook(postgres_conn_id=default_args["conn_id"]).copy_expert(
            'COPY dl.tmp_stock_ozon FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')',
            f'{csvfile.name}')
        os.remove(csvfile.name)
        return skus

    def ozon_extract_product_extra_info(clientId, token, prodict_ids) -> dict:
        logging.info("Extract product info")
        resp = request_repeater('POST', f"https://api-seller.ozon.ru/v3/products/info/attributes", headers={
            "Client-Id": clientId,
            "Api-Key": token,
            "Content-Type": "application/json",
        }, json={
            "filter": {
                "product_id": list(prodict_ids),
                "visibility": "ALL"
            },
            "limit": 1000,
            "sort_dir": "ASC"
        })
        items = resp.json()['result']
        result = dict()
        for item in items:
            attributes = item['attributes']
            for attribute in attributes:
                if attribute['attribute_id'] == 85 and len(attribute['values']) > 0:
                    brand = attribute['values'][0]['value']
                if attribute['attribute_id'] == 8229 and len(attribute['values']) > 0:
                    category = attribute['values'][0]['value']
            result[item['id']] = {
                'brand': brand,
                'category': category
            }
        logging.info("Extract product info: %s", len(result))
        return result

    @task
    def ozon_extract_product_info(owner, clientId, token, skus1, skus2) -> None:
        skus = set(skus1.union(skus2))
        pageSize = 1000
        offset = 0
        size = len(skus)
        arr = array('i', skus)
        csvfile = open(f'{default_args["work_dir"]}{uuid.uuid4()}.{owner.lower()}.product.csv', 'w')
        try:
            spamwriter = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            while offset < size:
                values = arr[offset:offset + pageSize]
                resp = request_repeater('POST', url=f"https://api-seller.ozon.ru/v2/product/info/list", headers={
                    "Client-Id": clientId,
                    "Api-Key": token,
                    "Content-Type": "application/json",
                }, json={
                    "sku": list(values)
                })
                items = resp.json()['result']['items']
                product_ids = map(lambda x: x['id'], items)
                extra_info = ozon_extract_product_extra_info(clientId, token, product_ids)
                for item in items:
                    if item['sku'] == 0:
                        sku = item['fbo_sku']
                    else:
                        sku = item['sku']
                    ei = extra_info[item['id']]
                    spamwriter.writerow(
                        [owner, item['barcode'], item['created_at'], sku, item['marketing_price'],
                         item['min_ozon_price'],
                         item['min_price'], item['offer_id'], item['old_price'], item['premium_price'],
                         item['price'], item['name'],
                         ei['brand'], ei['category']
                         ]
                    )
                offset += pageSize
            csvfile.close()
            PostgresHook(postgres_conn_id=default_args["conn_id"]).copy_expert(
                f"COPY dl.tmp_product_info_ozon FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', HEADER FALSE, QUOTE E'\\b')",
                f'{csvfile.name}')
        finally:
            os.remove(csvfile.name)
        logging.info("Skus: %s", skus)

    @task
    def apply_data() -> None:
        dateToStr = get_current_context()['params']['dateTo']
        dateTo = datetime.strptime(dateToStr, '%Y-%m-%d')
        offset_days = default_args["offset_days"]
        dateFrom = (dateTo - timedelta(days=offset_days)).strftime('%Y-%m-%d')
        logging.info(f"Apply data {dateTo} from {dateFrom}")
        PostgresHook(postgres_conn_id=default_args["conn_id"]).run(
            f"call dl.apply_orders(to_date('{dateFrom}', 'YYYY-MM-DD'))")

    orgs = PostgresHook(postgres_conn_id="database").get_records(
        "SELECT owner_code, password, client_id, source FROM ml.owner_marketplace t")
    clean_task = clean_and_init()
    apply_data_task = apply_data()

    group = dict()
    for org in orgs:
        if org[0] not in group:
            group[org[0]] = True
            start_task = start.override(task_id=f"{str(org[0]).lower()}_start")()
            group[org[0]] = start_task
        else:
            start_task = group[org[0]]
        clean_task >> start_task

        if org[3] == 'WB':
            wb_task = start.override(task_id=f"{str(org[0]).lower()}_wb")()
            start_task >> wb_task
            wb_extract_stock_task = wb_extract_stock.override(task_id=f"{str(org[0]).lower()}_wb_extract_stock")(org[0], org[1])
            wb_extract_orders_task = wb_extract_orders.override(task_id=f"{str(org[0]).lower()}_wb_extract_orders")(
                org[0], org[1])
            wb_extract_sales_task = wb_extract_sales.override(task_id=f"{str(org[0]).lower()}_wb_extract_sales")(org[0],
                                                                                                                 org[1])
            wb_task >> wb_extract_stock_task >> apply_data_task
            wb_task >> wb_extract_sales_task >> apply_data_task
            wb_task >> wb_extract_orders_task >> apply_data_task
        else:
            ozon_task = start.override(task_id=f"{str(org[0]).lower()}_ozon")()
            start_task >> ozon_task
            ozon_extract_stock_task = ozon_extract_stock.override(task_id=f"{str(org[0]).lower()}_ozon_extract_stock")(
                org[0], org[2], org[1])
            ozon_extract_orders_task = ozon_extract_orders.override(
                task_id=f"{str(org[0]).lower()}_ozon_extract_orders")(org[0], org[2], org[1])

            ozon_task >> ozon_extract_orders_task
            ozon_task >> ozon_extract_stock_task
            ozon_extract_product_info(org[0], org[2], org[1], ozon_extract_orders_task,
                                      ozon_extract_stock_task) >> apply_data_task
    # clean_task >> wb_extract_data_task >> wb_transform_data_task


dag_run = orders()
