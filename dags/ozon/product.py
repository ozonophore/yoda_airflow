import logging

from dags import httpclient
from dags.ozon.ozon import __create_header


def __extract_attributes(clientId: str, token: str, prodict_ids: list) -> dict:
    r"""
    Получение атрибутов товаров
        clientId - идентификатор клиента
        token - токен
        prodict_ids - список id товаров
        return - словарь id товара - атрибуты
    """
    logging.info("Extract product attributes")
    resp = httpclient.post(f"https://api-seller.ozon.ru/v3/products/info/attributes",
                           headers=__create_header(clientId, token), json={
            "filter": {
                "product_id": prodict_ids,
                "visibility": "ALL"
            },
            "limit": 1000,
            "sort_dir": "ASC"
        })
    resp.raise_for_status()
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


def extract_product_info(id: int, writer, owner: str, clientId: str, token: str, ids: set) -> None:
    r"""
    Получение информации о товарах
        writer - объект для записи данных о товарах
        owner - владелец
        clinetId - идентификатор клиента
        token - токен
        ids - список id товаров
        return - имя файла с данными
    """
    header = __create_header(clientId, token)
    logging.info("Extract product info")

    index = 0
    offset = 999

    lst = list(ids)
    loaded = set()
    while True:
        items = lst[index:offset]
        resp = httpclient.post('https://api-seller.ozon.ru/v2/product/info/list', headers=header, json={
            "sku": items
        })
        resp.raise_for_status()
        data = resp.json()
        prodict_ids = set()
        for item in data["result"]["items"]:
            prodict_ids.add(item["id"])
        attributes = __extract_attributes(clientId, token, list(prodict_ids))
        for item in data["result"]["items"]:
            sku = item['sku']
            ei = attributes[item['id']]
            if sku == 0:
                sku = item['fbo_sku']
                if sku not in loaded:
                    writer.writerow([
                        owner,
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
                        ei['category'],
                        id
                    ])
                    loaded.add(sku)
                sku = item['fbs_sku']
            if sku not in loaded:
                writer.writerow([
                    owner,
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
                    ei['category'],
                    id
                ])
                loaded.add(sku)
        if offset > len(items):
            break
        index += offset
        offset += offset

def get_sku_by_product_id(clientId: str, token: str, product_ids: set) -> dict:
    header = __create_header(clientId, token)
    logging.info("Extract product info")

    index = 0
    offset = 999

    lst = list(product_ids)
    loaded = dict()
    while True:
        items = lst[index:offset]
        resp = httpclient.post('https://api-seller.ozon.ru/v2/product/info/list', headers=header, json={
            "product_id": items
        })
        resp.raise_for_status()
        data = resp.json()
        for item in data["result"]["items"]:
            sku = item['sku']
            if sku == 0:
                sku = item['fbo_sku']
                loaded[str(sku)] = item['barcode']
                sku = item['fbs_sku']
            loaded[str(sku)] = item['barcode']
        if offset > len(items):
            break
        index += offset
        offset += offset
    return loaded

def get_product_ids(clientId: str, token: str) -> set:
    r"""
    Получение списка id товаров
        clientId - идентификатор клиента
        token - токен
        return - список id товаров
    """
    header = __create_header(clientId, token)
    limit = 1000
    params = {
        "filter": {
            "visibility": "ALL"
        },
        "last_id": "",
        "limit": limit
    }
    product_ids = set()
    while True:
        req = httpclient.post("https://api-seller.ozon.ru/v3/product/list", headers=header, json=params)
        req.raise_for_status()
        result = req.json()["result"]
        items = result["items"]
        for item in items:
            product_ids.add(item["product_id"])
        size = len(items)
        last_id = result["last_id"]
        params["last_id"] = last_id
        if size != limit:
            return product_ids