import csv
import json
import logging
import os
import uuid
from array import array
from datetime import datetime, timedelta
from time import sleep

import requests
import urllib3
from airflow.decorators import task
from airflow.exceptions import AirflowNotFoundException
from airflow.models import Param
from airflow.models.dag import dag
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import today

default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=60),
    'work_dir': 'data/',
    'conn_id': 'database',
    'offset_days': 45,
}

max_retries = 10  # Максимальное количество попыток
retry_delay = 30  # Задержка в секундах между попытками
group_days = 20  # Количество дней для группповой загрузки

http = urllib3.PoolManager()


def write_statistic(source, owner, type_data, count):
    sttm = PostgresHook(
        postgres_conn_id=default_args["conn_id"]
    )
    sttm.run(f"merge into ml.download_statistic ds "
             f"using (select '{source}' source, '{owner}' owner_code, '{type_data}' as type, {count} qnt) o " +
             f"on ds.source = o.source and ds.owner_code = o.owner_code and ds.type = o.type " +
             f"when matched then update set qnt = o.qnt " +
             f"when not matched then " +
             f"insert (source, owner_code, type, qnt) " +
             f"values (o.source, o.owner_code, o.type, o.qnt);")


def get_statistic(source, owner, type_data) -> int:
    sttm = PostgresHook(
        postgres_conn_id=default_args["conn_id"]
    )
    rec = sttm.get_first("select qnt from ml.download_statistic ds " +
                         f"where ds.source = '{source}' and ds.owner_code = '{owner}' and ds.type = '{type_data}';")
    if rec is None:
        return group_days
    count_day = rec[0] / default_args["offset_days"]
    if count_day == 0:
        return 15
    waist_days = int(60000 / count_day)
    if waist_days > default_args["offset_days"]:
        waist_days = default_args["offset_days"]
    return waist_days


def download_large_json(url, method="GET", headers=None):
    """
    Загружает большой JSON-файл по HTTP с возможностью повторных попыток.

    :param url: URL, по которому находится JSON-файл
    :param method: Метод запроса ("GET" или "POST")
    :param max_retries: Максимальное количество попыток
    :param retry_delay: Задержка между попытками в секундах
    :return: Загруженные JSON-данные или None в случае неудачи
    """

    for _ in range(max_retries):
        try:
            response = http.request(method=method, url=url, headers=headers, preload_content=False)

            if response.status == 200:
                # Используйте iter_content для потокового чтения данных
                json_data = []
                for chunk in response.stream(8192):
                    json_data.append(chunk)
                return json.loads(b''.join(json_data).decode('utf-8'))
            else:
                logging.error(
                    f"Ошибка при выполнении HTTP-запроса (попытка {max_retries - _}):{response.status} {response.data}")

        except urllib3.exceptions.HTTPError as e:
            logging.error(f"Ошибка при выполнении HTTP-запроса (попытка {max_retries - _}): %s", e)

        if _ < max_retries - 1:
            logging.info(f"Повторная попытка через {retry_delay} секунд...")
            sleep(retry_delay)

    raise Exception(f"Не удалось выполнить запрос после {max_retries} попыток.")


def request_repeater_large(method, url, **kwargs):
    for _ in range(max_retries):
        try:
            resp = requests.request(method, url, stream=True, **kwargs)
            if resp.status_code == 200:
                path = f'{default_args["work_dir"]}{uuid.uuid4()}.data'
                size = 0
                f = open(path, 'wb')
                try:
                    size += f.write(resp.raw.read())
                    f.close()
                    with open(path, 'r') as fr:
                        logging.info(f"Parse json {path}")
                        data = json.load(fr)
                        fr.close()
                    return data
                finally:
                    os.remove(path)
            else:
                logging.info(f"Попытка: {max_retries - _} Код: {resp.status_code} Сообщение: {resp.text}")
        except Exception as e:
            logging.error(f"Попытка: {max_retries - _} Ошибка: {e}")
        if _ < max_retries - 1:
            logging.info(f"Повторная попытка через {retry_delay} секунд...")
            sleep(retry_delay)
    raise Exception(f"Не удалось выполнить запрос после {max_retries} попыток.")


def request_repeater(method, url, **kwargs) -> requests.Response:
    attempt = max_retries
    while True:
        resp = requests.request(method, url, **kwargs)
        if resp.status_code == 200:
            return resp
        if attempt == 0:
            resp.raise_for_status()
        logging.info(f"Attemption: {attempt} code: {resp.status_code} message: {resp.text}")
        attempt -= 1
        sleep(30)


def refresh_token(clientCode, source) -> dict:
    r = dict()
    rec = PostgresHook(postgres_conn_id="database").get_first(
        f"SELECT password, client_id FROM ml.owner_marketplace t where t.owner_code = '{clientCode}' and t.source='{source}'")
    if len(rec) == 0:
        raise AirflowNotFoundException(f"Client code {clientCode} not found")
    logging.info(f"Client code: {clientCode} password: {rec[0]} client_id: {rec[1]}")
    r['password'] = rec[0]
    r['client_id'] = rec[1]
    return r


@dag(
    start_date=datetime(2023, 11, 8),
    schedule_interval="0 1 * * *",
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=4,
    catchup=False,
    params={
        "dateTo": Param(today().strftime("%Y-%m-%d"), title="Дата до", format="date",
                        type="string", description="Дата до", ),
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

    def get_file_size(file_path):
        return os.path.getsize(file_path)

    @task
    def wb_extract_sales(owner) -> None:
        token = refresh_token(owner, "WB")['password']
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(f"delete from dl.tmp_sale where owner_code = '{owner}';")

        offset_days = default_args["offset_days"]
        dateToStr = get_current_context()['params']['dateTo']
        dateTo = datetime.strptime(dateToStr, '%Y-%m-%d')
        startdate = dateTo - timedelta(days=offset_days)
        current_file_size = 0
        current_file_index = 0  # Индекс текущего файла
        max_file_size = 30 * 1024 * 1024  # Максимальный размер файла (30 МБ)
        filename_base = uuid.uuid4()

        try:
            csvfile = None

            days = offset_days
            count = 0
            group_days = get_statistic("WB", owner, "sale")

            while True:
                logging.info("Days: %s from %s", days, group_days)

                if days <= group_days:
                    flag = 0
                else:
                    flag = 1

                logging.info("Date from: %s", startdate)
                resp = request_repeater_large(method="GET",
                                              url=f'https://statistics-api.wildberries.ru/api/v1/supplier/sales?dateFrom={startdate}&flag={flag}',
                                              headers={"Authorization": f"Bearer {token}"})

                if not resp:
                    break

                size = len(resp)
                count += size

                if csvfile is None or current_file_size > max_file_size:
                    # Если текущий файл превысил максимальный размер, создаем новый файл
                    if csvfile:
                        csvfile.close()
                    current_file_index += 1
                    csvfile_path = f'{default_args["work_dir"]}{filename_base}.{owner}.sale.{current_file_index}.csv'
                    csvfile = open(csvfile_path, 'w')

                spamwriter = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                logging.info("Количество записей: %s", size)

                for row in resp:
                    if row['srid'] == 0:
                        continue
                    row_date = datetime.strptime(row['date'], '%Y-%m-%dT%H:%M:%S')
                    if row_date < startdate:
                        continue  # skip old data

                    spamwriter.writerow([
                        row['date'], owner, row['lastChangeDate'], str(row['warehouseName']).upper(), row['countryName'],
                        row['oblastOkrugName'], row['regionName'], row['supplierArticle'], row['barcode'],
                        row['category'], row['subject'], row['brand'], row['techSize'], row['incomeID'],
                        row['isSupply'], row['isRealization'], row['totalPrice'], row['discountPercent'],
                        row['spp'], row['forPay'], row['finishedPrice'], row['priceWithDisc'], row['saleID'],
                        row['sticker'], row['gNumber'], 0, row['srid'], row['nmId']
                    ])
                csvfile.flush()
                current_file_size += get_file_size(csvfile_path)

                if startdate > dateTo or days <= group_days:
                    break
                else:
                    startdate += timedelta(days=1)
                    days -= 1
                    sleep(15)

            if csvfile:
                csvfile.close()

            write_statistic("WB", owner, "sale", count)

            # Копирование всех файлов в базу данных
            for file_index in range(1, current_file_index):
                csvfile_path = f'{default_args["work_dir"]}{filename_base}.{owner}.sale.{file_index}.csv'
                PostgresHook(
                    postgres_conn_id=default_args["conn_id"],
                ).copy_expert(
                    'COPY dl.tmp_sale FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')',
                    csvfile_path)
                os.remove(csvfile_path)

        except Exception as e:
            logging.error(f"Ошибка загрузки: {str(e)}")
            raise e
        finally:
            if csvfile:
                csvfile.close()
                # Удаление всех созданных файлов, если произошла ошибка
            for file_index in range(1, current_file_index):
                csvfile_path = f'{default_args["work_dir"]}{filename_base}.{owner}.sale.{file_index}.csv'
                if os.path.exists(csvfile_path):
                    os.remove(csvfile_path)

    def prepare_wb_date(owner, dateFrom, csvfile, token, flag) -> int:
        logging.info("Date from: %s", dateFrom)
        startdate = dateFrom.strftime('%Y-%m-%dT%H:%M:%S')

        url = f'http://statistics-api.wildberries.ru/api/v1/supplier/orders?dateFrom={startdate}&flag={flag}'
        json = request_repeater_large("GET",
                                      url,
                                      headers={"Authorization": f"Bearer {token}"})
        size = len(json)
        logging.info("Size: %s", size)
        if size == 0:
            return 0
        maxDate = dateFrom
        spamwriter = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        i = 0
        allCount = 0
        for row in json:
            allCount += 1
            if datetime.strptime(row['date'], '%Y-%m-%dT%H:%M:%S') < dateFrom:
                continue  # skip old data
            maxDate = max(maxDate, datetime.strptime(row['lastChangeDate'], '%Y-%m-%dT%H:%M:%S'))
            spamwriter.writerow(
                [row['date'], owner, row['lastChangeDate'], row['supplierArticle'], row['techSize'],
                 row['barcode'], row['totalPrice'], row['discountPercent'], str(row['warehouseName']).upper(),
                 row['regionName'], row['incomeID'], 0, row['subject'], row['category'],
                 row['brand'], row['isCancel'], row['cancelDate'], row['gNumber'], row['sticker'], row['srid'],
                 row['orderType'], row['nmId'], row['spp'], row['finishedPrice'], row['priceWithDisc'],
                 row['countryName'], row['oblastOkrugName'], row['regionName']])
            i += 1
        logging.info("Inserted: %s from %s", i, allCount)
        return allCount

    @task
    def wb_extract_orders(owner) -> None:
        token = refresh_token(owner, "WB")['password']
        PostgresHook(
            postgres_conn_id=default_args["conn_id"]
        ).run(f"delete from dl.tmp_orders_wb where owner_code = '{owner}';")
        dateToStr = get_current_context()['params']['dateTo']
        dateTo = datetime.strptime(dateToStr, '%Y-%m-%d')
        filename = uuid.uuid4()
        csvfile = open(f'{default_args["work_dir"]}{filename}.{owner}.order.wb.csv', 'w')
        try:
            offset_days = default_args["offset_days"]
            startDate = dateTo - timedelta(days=offset_days)
            # while startDate is not None:
            #     startDate = prepare_wb_date(owner, startDate, csvfile, token)
            days = offset_days
            count = 0
            group_days = get_statistic("WB", owner, "order")
            while True:
                logging.info("Days %s from %s", days, group_days)
                if days <= group_days:
                    flag = 0
                else:
                    flag = 1
                count += prepare_wb_date(owner, startDate, csvfile, token, flag)
                if startDate > dateTo or days <= group_days:
                    break
                else:
                    startDate = startDate + timedelta(days=1)
                    days -= 1
                    sleep(10)
            logging.info("Downloaded: %s", count)
            if count == 0:
                raise AirflowNotFoundException("No data")
            write_statistic("WB", owner, "order", count)
            csvfile.close()

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
                'region_name) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')',
                f'{csvfile.name}')
        finally:
            os.remove(csvfile.name)

    @task
    def wb_extract_stock(owner) -> None:
        token = refresh_token(owner, "WB")['password']
        dateToStr = get_current_context()['params']['dateTo']
        dateTo = datetime.strptime(dateToStr, '%Y-%m-%d')
        offset_days = default_args["offset_days"]
        dateFrom = dateTo - timedelta(days=offset_days)
        dateFromStr = dateFrom.strftime('%Y-%m-%d')
        # resp = request_repeater("GET",
        #                         f"https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={dateFromStr}",
        #                         headers={"Authorization": f"Bearer {token}"})
        # items = resp.json()

        items = request_repeater_large("GET",
                                       f"https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={dateFromStr}",
                                       headers={"Authorization": f"Bearer {token}"})
        if len(items) == 0:
            return None
        csvfile = open(f'{default_args["work_dir"]}{uuid.uuid4()}.{owner}.stock.wb.csv', 'w')
        try:
            createAt = datetime.now().strftime('%Y-%m-%d')
            spamwriter = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            for item in items:
                spamwriter.writerow(
                    [owner, item['lastChangeDate'], str(item['warehouseName']).upper(), item['supplierArticle'], item['barcode'],
                     item['quantity'], item['inWayToClient'], item['inWayFromClient'], item['quantityFull'],
                     item['category'], item['subject'], item['brand'], item['Price'],
                     item['Discount'], item['isSupply'], item['isRealization'], item['nmId'], createAt])
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
        startDate = maxDate
        product_count = 0
        try:
            while True:
                if offset > 20000:
                    offset = 0
                    sinceDate = (maxDate + timedelta(milliseconds=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    logging.info("Since date: %s", sinceDate)
                resp = request_repeater('POST', "https://api-seller.ozon.ru/v2/posting/fbo/list", headers=headers,
                                        json={
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
                for item in items:
                    if datetime.strptime(item['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ') < startDate:
                        continue
                    maxDate = max(maxDate, datetime.strptime(item['in_process_at'], '%Y-%m-%dT%H:%M:%S.%fZ'))
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
                             str(analytics_data['warehouse_name']).upper(),
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
            logging.info("Inserted: %d", product_count)
        finally:
            os.remove(csvfile.name)
        return skus

    @task
    def ozon_extract_stock(owner, clientId, token) -> set:
        headers = {
            "Client-Id": clientId,
            "Api-Key": token,
            "Content-Type": "application/json",
        }
        offset = 0
        pageSize = 1000
        skus = set()
        csvfile = open(f'{default_args["work_dir"]}{uuid.uuid4()}.{owner.lower()}.stock.ozon.csv', 'w')
        try:
            while True:
                resp = request_repeater('POST', "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses",
                                        headers=headers
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
                createAt = datetime.now().strftime('%Y-%m-%d')
                for row in items:
                    skus.add(row['sku'])
                    spamwriter.writerow(
                        [row['sku'], row['item_code'], row['item_name'], row['free_to_sell_amount'],
                         row['promised_amount'],
                         row['reserved_amount'], str(row['warehouse_name']).upper(), owner, createAt]
                    )

                if size < pageSize:
                    break
                offset += pageSize
            csvfile.close()
            PostgresHook(postgres_conn_id=default_args["conn_id"]).copy_expert(
                'COPY dl.tmp_stock_ozon FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', HEADER FALSE, QUOTE E\'\\b\')',
                f'{csvfile.name}')
        finally:
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
                        [owner,
                         item['barcode'],
                         item['created_at'],
                         sku,
                         item['marketing_price'],
                         item['min_ozon_price'],
                         item['min_price'],
                         item['offer_id'],
                         item['old_price'],
                         item['premium_price'],
                         item['price'],
                         item['name'],
                         ei['brand'],
                         ei['category']
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
    def apply_stock() -> None:
        stock_date = today().add(days=-1).strftime('%Y-%m-%d')
        logging.info(f"Apply stock {stock_date}")
        PostgresHook(postgres_conn_id=default_args["conn_id"]).run(
            f"call dl.apply_stock(to_date('{stock_date}', 'YYYY-MM-DD'))")

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
    apply_stock_task = apply_stock()

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
            wb_extract_stock_task = wb_extract_stock.override(task_id=f"{str(org[0]).lower()}_wb_extract_stock")(org[0])
            wb_extract_orders_task = wb_extract_orders.override(task_id=f"{str(org[0]).lower()}_wb_extract_orders")(
                org[0])
            wb_extract_sales_task = wb_extract_sales.override(task_id=f"{str(org[0]).lower()}_wb_extract_sales")(org[0])
            wb_task >> wb_extract_stock_task >> apply_stock_task
            wb_task >> wb_extract_orders_task >> wb_extract_sales_task >> apply_data_task
        else:
            ozon_task = start.override(task_id=f"{str(org[0]).lower()}_ozon")()
            start_task >> ozon_task
            ozon_extract_stock_task = ozon_extract_stock.override(task_id=f"{str(org[0]).lower()}_ozon_extract_stock")(
                org[0], org[2], org[1])
            ozon_extract_orders_task = ozon_extract_orders.override(
                task_id=f"{str(org[0]).lower()}_ozon_extract_orders")(org[0], org[2], org[1])

            ozon_task >> ozon_extract_orders_task
            ozon_task >> ozon_extract_stock_task

            ozon_extract_product_info_task = ozon_extract_product_info.override(
                task_id=f"{str(org[0]).lower()}_ozon_extract_product_info")(org[0], org[2], org[1],
                                                                            ozon_extract_orders_task,
                                                                            ozon_extract_stock_task)

            ozon_extract_product_info_task >> apply_data_task
            ozon_extract_product_info_task >> apply_stock_task
    # clean_task >> wb_extract_data_task >> wb_transform_data_task


dag_run = orders()
