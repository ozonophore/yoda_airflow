import logging
from datetime import date, datetime

from dags import httpclient
from dags.ozon.ozon import __create_header


def _pars_orders_fbs(id: int, writer, owner: str, data, dateFrom: date) -> set:
    r"""
    Парсинг данных о заказах и запись в файл
        id - идентификатор процесса
        writer - объект для записи
        data - данные
        dateFrom - дата с которой начинается выборка
        return - список id
    """
    skus = set()
    for item in data:
        in_process_at = datetime.strptime(item['in_process_at'], '%Y-%m-%dT%H:%M:%SZ')
        if in_process_at.date() < dateFrom:
            continue
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
                 item['order_id'],
                 item['order_number'],
                 item['posting_number'],
                 item['status'],
                 item['in_process_at'],
                 item['in_process_at'],
                 product['sku'],
                 product['name'],
                 product['quantity'],
                 product['offer_id'],
                 product['price'],
                 analytics_data['region'],
                 analytics_data['city'],
                 "FBS",
                 #analytics_data['warehouse'],
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
                 "FBS"
                 ])
            product_count += 1
    return skus


def extract_orders_fbs(id: int, writer, owner: str, clientId: str, token: str, dateFrom: date,
                       dateTo: date) -> set:
    r"""
    Запрос данных о заказах со склада FBS через API(https://api-seller.ozon.ru/v3/posting/fbs/list)
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
            "to": f"{dateTo.strftime('%Y-%m-%d')}T00:00:00Z"
        },
        "limit": limit,
        "offset": offset,
        "with": {
            "analytics_data": True,
            "barcodes": True,
            "financial_data": True,
            "translit": True
        }
    }
    skus = set()
    while True:
        resp = httpclient.post("https://api-seller.ozon.ru/v3/posting/fbs/list", json=req_params, headers=header)
        resp.raise_for_status()
        data = resp.json()
        result = data['result']
        has_next = result["has_next"]
        logging.info(f"Offset: {offset} limit: {limit} has_next: {has_next} rows: {len(result['postings'])}")
        skus = skus.union(_pars_orders_fbs(id, writer, owner, result['postings'], dateFrom))
        if has_next is False:
            break
        offset += limit
        req_params["offset"] = offset
    return skus
