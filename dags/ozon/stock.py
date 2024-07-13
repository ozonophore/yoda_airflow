import datetime
import logging

from dags import httpclient


def extract_stock_sale(id: int, stockDate: datetime.date, writer, owner: str, skus: dict, clientId: str, token: str) -> None:
    headers = {
        "Client-Id": clientId,
        "Api-Key": token,
        "Content-Type": "application/json",
    }
    offset = 0
    pageSize = 1000
    # skus = set()
    while True:
        params = {
            "warehouse_type": "ALL",
            "limit": pageSize,
            "offset": offset
        }
        resp = httpclient.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses",
                               headers=headers,
                               json=params
                               )
        resp.raise_for_status()
        items = resp.json()['result']
        rows = items['rows']
        size = len(rows)
        logging.info("Size: %s", size)
        for row in rows:
            # skus.add(row['sku'])
            writer.writerow(
                [owner,
                 stockDate,
                 row['sku'],
                 None,
                 None,
                 None,
                 None,
                 None,
                 None,
                 None,
                 row['item_name'],
                 None,
                 None,
                 str(row['warehouse_name']).upper(),
                 None,
                 None,
                 row['free_to_sell_amount'],
                 id,
                 skus.get(row['sku']),
                 ]
            )

        if size < pageSize:
            break
        offset += pageSize

def extract_stock(id: int, stockDate: datetime.date, writer, owner: str, skus: dict, clientId: str, token: str) -> None:
    header = {
        "Client-Id": clientId,
        "Access-Key": token,
        "Content-Type": "application/json",
    }
    listSkus = list(skus.keys())
    index = 0
    offset = 100
    params = {
        "options": {
            "with_seller_currency": True
        },
        "skus": []
    }
    end = offset
    logging.info("Skus size: %s", len(listSkus))
    while True:
        logging.info("Index: %s end: %s", index, end)
        skusPart = listSkus[index:end]
        params["skus"] = skusPart
        logging.info("Params: %s", params)
        resp = httpclient.post("https://xapi.ozon.ru/seller-stats-api/v1/stock", headers=header, json=params)
        resp.raise_for_status()
        items = resp.json()['result']
        logging.info("Result: %s", items)
        for item in items:
            category = item['category']
            price = item['price']
            info = item['info']
            stocks = item['stock']
            for stock in stocks:
                warehouse = stock['warehouse']
                writer.writerow(
                    [owner,
                     stockDate,
                     item['sku'],
                     item['method'],
                     category['name'],
                     price['currency'],
                     price['base_price'],
                     price['discount_price'],
                     price['premium_price'],
                     price['ozon_card_price'],
                     info['name'],
                     info['brand'],
                     info['seller'],
                     str(warehouse['name']).upper(),
                     warehouse['region'],
                     warehouse.get('id'),
                     stock['quantity'],
                     id,
                     skus[item['sku']],
                     ]
                )

        if offset > len(items):
            break
        index += offset
        end += offset
