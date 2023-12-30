import datetime
import logging

from dags import httpclient


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
    while True:
        items = listSkus[index:offset]
        params["skus"] = items
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
        offset += offset
