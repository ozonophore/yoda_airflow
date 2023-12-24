import logging
import re
from datetime import date, datetime, timedelta

from dags import httpclient
from dags.ozon.ozon import __create_header


def remove_microseconds(dateStr: str) -> str:
    m = re.search(r'^.*(\.\d*Z)$', dateStr)
    if m is not None:
        newDate = dateStr.replace(m.group(1), 'Z')
        return newDate
    return dateStr

def _pars_orders_fbo(id: int, writer, owner: str, data, dateFrom: datetime) -> (set, datetime):
    r"""
    Парсинг данных о заказах и запись в файл
        id - идентификатор процесса
        writer - объект для записи
        data - данные
        dateFrom - дата с которой начинается выборка
        return - список id
    """
    skus = set()
    maxDate = dateFrom
    for item in data:
        created_at = datetime.strptime(remove_microseconds(item['created_at']), '%Y-%m-%dT%H:%M:%SZ')
        if created_at < dateFrom:
            continue
        maxDate = max(maxDate, datetime.strptime(remove_microseconds(item['in_process_at']), '%Y-%m-%dT%H:%M:%SZ'))
        analytics_data = item['analytics_data']
        financial_dict = dict()
        if item['financial_data'] is not None:
            financial_data = item['financial_data']['products']
            financial_dict = dict()
            for fd in financial_data:
                financial_dict[fd['product_id']] = fd
        products = item['products']
        product_count = 0
        for index, product in enumerate(products, start=0):
            fd = financial_dict[product['sku']]
            skus.add(product['sku'])
            writer.writerow(
                            [owner,
                             item['order_id'], item['order_number'],
                             item['posting_number'],
                             item['status'],
                             item['created_at'],
                             item['in_process_at'],
                             product['sku'],
                             product['name'],
                             product['quantity'],
                             product['offer_id'],
                             product['price'],
                             analytics_data['region'],
                             analytics_data['city'],
                             str(analytics_data['warehouse_name']).upper(),
                             analytics_data['warehouse_id'],
                             fd['commission_amount'],
                             fd['commission_percent'],
                             fd['payout'],
                             fd['product_id'],
                             fd['old_price'],
                             fd['total_discount_value'],
                             fd['total_discount_percent'],
                             fd['client_price'],
                             id,
                             "FBO"
                             ])
            product_count += 1
    return skus, maxDate


def extract_orders_fbo(id: int, writer, owner: str, clientId: str, token: str, dateFrom: date,
                       dateTo: date) -> set:
    r"""
Запрос данных о заказах со склада FBO через API(https://api-seller.ozon.ru/v2/posting/fbo/list)
        id - идентификатор процесса
        writer - объект для записи данных о заказах
        owner - владелец
        clientId - идентификатор клиента
        token - токен
        dateFrom - дата с которой начинается выборка
        dateTo - дата по которую выбираем
        return - список id товаров
    """
    header = __create_header(clientId, token)
    offset = 0
    limit = 999
    req_params = {
        "dir": "ASC",
        "filter": {
            "since": f"{dateFrom.strftime('%Y-%m-%d')}T00:00:00Z",
            "status": "",
            "to": f"{dateTo.strftime('%Y-%m-%d')}T00:00:00Z"
        },
        "limit": limit,
        "offset": offset,
        "translit": False,
        "with": {
            "analytics_data": True,
            "financial_data": True
        }
    }
    skus = set()
    maxDateFrom = datetime(dateFrom.year, dateFrom.month, dateFrom.day)
    datetimeFrom = datetime(dateFrom.year, dateFrom.month, dateFrom.day)
    while True:
        if offset > 20000:
            offset = 0
            datetimeFrom = maxDateFrom + timedelta(seconds=1)
            logging.info("Since date: %s", datetimeFrom)
            req_params["filter"]["since"] = f"{datetimeFrom.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            req_params["offset"] = offset

        resp = httpclient.post("https://api-seller.ozon.ru/v2/posting/fbo/list", json=req_params, headers=header)
        resp.raise_for_status()
        data = resp.json()
        result = data['result']
        size = len(result)
        if size == 0:
            break
        logging.info(f"Offset: {offset} limit: {limit} rows: {size}")
        newSkus, maxDateFrom = _pars_orders_fbo(id, writer, owner, result, datetime(dateFrom.year, dateFrom.month, dateFrom.day))
        skus = skus.union(newSkus)
        if size < limit:
            break
        offset += limit
        req_params["offset"] = offset
    return skus