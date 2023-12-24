import csv
import logging
import os.path
import re
import shutil
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date
from time import sleep

from dateutil.relativedelta import relativedelta

from dags import httpclient

__file_pattern = "^orders_(fbo|fbs).*.csv"


def prepare_datafile(dirName: str, owner: str, dateFrom: date, dateTo: date) -> str:
    files = os.listdir(dirName)
    with open(f"{dirName}/orders_{dateFrom.strftime('%Y%m%d')}_{dateTo.strftime('%Y%m%d')}.data", 'r', newline='',
              encoding="utf-8") as ata_file:
        writer = csv.writer(ata_file, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for file in files:
            m = re.search(__file_pattern, file)
            if m is None:
                continue
            wh_type = m.group(1).upper()
            # with csv.reader(open(f"{dirName}/{file}", 'r', newline='', encoding="utf-8"), delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL) as reader:
            #     for item in reader:
            #         writer.writerow(
            #             [owner,
            #              item['order_id'], item['order_number'], item['posting_number'], item['status'],
            #              item['created_at'], item['in_process_at'], product['sku'], product['name'],
            #              product['quantity'],
            #              product['offer_id'],
            #              product['price'], analytics_data['region'], analytics_data['city'],
            #              str(analytics_data['warehouse_name']).upper(),
            #              analytics_data['warehouse_id'], fd['commission_amount'], fd['commission_percent'],
            #              fd['payout'], fd['product_id'], fd['old_price'], fd['total_discount_value'],
            #              fd['total_discount_percent'], fd['client_price']
            #              ])

    #
    # f = open(fileName, 'a', encoding='utf-8')
    # try:
    #     writer = csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    #     for key, value in data.items():
    #         writer.writerow([key, value["name"], value["barcode"], value["brand"], value["category"]])
    # finally:
    #     f.close()



def scan_dir(dirName: str) -> set:
    r"""
    Парсинг директории
        dirName - имя директории
        return - список id
    """
    files = os.listdir(dirName)
    ids = set()
    for file in files:
        if file.endswith(".csv"):
            r = __pars_id_csv(f"{dirName}/{file}")
            ids = ids.union(r)
    return ids


def __pars_id_csv(fileName: str) -> set:
    r"""
    Парсинг файла
        fileName - имя файла
        return - список id
    """
    result = set()
    with open(fileName, 'r', newline='', encoding='utf-8-sig') as csv_file:
        reader = csv.reader(csv_file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        i = 0
        for row in reader:
            if i == 0:
                i += 1
                continue
            result.add(row[9])
            i += 1
    return result


def __download(file: str, newFile: str) -> str:
    r"""
    Загрузка файла
        file - ссылка на файл
        newFile - имя файла для сохранения
        return - имя файла
    """
    resp = httpclient.get(file, allow_redirects=True)
    open(newFile, 'wb').write(resp.content)
    return newFile


def request_report(workDir: str, clientId: str, token: str, dateFrom: date, dateTo: date) -> str:
    r"""
    Получение отчета по заказам
        workDir - рабочая директория
        clientId - идентификатор клиента
        token - токен
        dateFrom - дата начала периода
        dateTo - дата окончания периода
        return - имя директории для сохранения отчета
    """
    dirName = f"{workDir}/orders_{dateFrom.strftime('%Y%m%d')}_{dateTo.strftime('%Y%m%d')}"
    if os.path.exists(dirName):
        shutil.rmtree(dirName)
    os.mkdir(dirName)
    pool = ThreadPoolExecutor(max_workers=2)
    pool.submit(request_report_by_schema, clientId, token, dateFrom, dateTo, dirName, "fbo")
    pool.submit(request_report_by_schema, clientId, token, dateFrom, dateTo, dirName, "fbs")
    pool.shutdown(wait=True)
    return dirName


r"""
Получение отчета по заказам за промежуточный период
    clientId - идентификатор клиента
    token - токен
    startDate - дата начала периода
    endDate - дата окончания периода
    dateTo - дата окончания основного периода
    dirName - имя директории для сохранения отчета
    schema - схема доставки( fbo, fbs)
"""


def request_report_by_schema(clientId: str, token: str, dateFrom: date, dateTo: date, dirName: str,
                             schema: str) -> None:
    startDate = dateFrom
    if dateFrom + relativedelta(months=3) > dateTo:
        endDate = dateTo
    else:
        endDate = dateFrom + relativedelta(months=3)
    while True:
        print(f"{schema} Start date: %s end date: %s", startDate, endDate)
        __request_report_by_shortperiod(clientId, token, startDate, endDate, dirName, schema)
        if endDate == dateTo:
            break
        startDate = endDate
        if startDate + relativedelta(months=3) > dateTo:
            endDate = dateTo
        else:
            endDate = endDate + relativedelta(months=3)
    print(f"{schema} Start date: %s end date: %s - Successful", startDate, endDate)


def __create_header(clinetId: str, apiKey: str) -> dict:
    r"""
    Создание заголовка запроса
        clinetId - идентификатор клиента
        apiKey - ключ
        return - заголовок запроса
    """
    return {
        "Client-Id": clinetId,
        "Api-Key": apiKey,
        "Content-Type": "application/json",
    }


def __request_report_by_shortperiod(clientId: str, token: str, dateFrom: date, dateTo: date, dirName: str,
                                    schema: str) -> None:
    data = {
        "filter": {
            "processed_at_from": f"{dateFrom.strftime('%Y-%m-%d')}T00:00:00.000Z",
            "processed_at_to": f"{dateTo.strftime('%Y-%m-%d')}T00:00:00.000Z",
            "delivery_schema": [
                schema
            ]
        },
        "language": "DEFAULT"
    }
    # "176640", "87016e04-99f6-48cb-aa73-305ca11828ae"
    header = __create_header(clientId, token)

    resp = httpclient.post("https://api-seller.ozon.ru/v1/report/postings/create", json=data
                           , headers=header)
    resp.raise_for_status()
    data = resp.json()
    logging.info(data)
    reportCode = data["result"]["code"]
    request = {
        "code": reportCode
    }
    i = 0
    while True:
        print("Start date: %s end date: %s", dateFrom, dateTo)
        print("Try %s", i)
        resp = httpclient.post("https://api-seller.ozon.ru/v1/report/info", json=request
                               , headers=header)
        resp.raise_for_status()
        data = resp.json()
        if data["result"]["status"] == "success":
            file = data["result"]["file"]
            newFile = f"{dirName}/orders_{schema}_{dateFrom.strftime('%Y%m%d')}_{dateTo.strftime('%Y%m%d')}.csv"
            __download(file, newFile)
            break
        if data["result"]["status"] == "failed":
            raise Exception("Report failed", data["result"]["error"])
        sleep(60)
        i += 1
        if i > 20:
            raise Exception("Report failed", data["result"]["error"])
