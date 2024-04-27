import logging
from datetime import datetime

import psycopg2

from dags import httpclient


class Product:
    def __init__(self, barcode, source, offer):
        self.barcode = barcode
        self.source = source
        self.offer = offer

    def __hash__(self):
        return hash((self.barcode, self.source, self.offer))

    def __eq__(self, other):
        return (self.barcode, self.source, self.offer) == (other.barcode, other.source, other.offer)


def write(writer, id, client_code, name, articul, category_name, brand_name, default_price, company_id, company_name,
          region_name, price, in_stock, discount_type, discount, offer_name, date, last_check_date, original_price,
          date_create):
    writer.writerow([
        id,
        client_code,
        name,
        articul,
        category_name,
        brand_name,
        default_price,
        company_id,
        company_name,
        region_name,
        price,
        in_stock,
        discount_type,
        discount,
        offer_name,
        date,
        last_check_date,
        original_price,
        date_create
    ])


def load_price(id: int, fileName: str, database, user, password, host, port: str) -> None:
    r"""
    Загрузка данных в БД
    """
    conn = psycopg2.connect(database=database,
                            user=user,
                            password=password,
                            host=host,
                            port=port)
    cur = conn.cursor()
    try:
        cur.execute("delete from dl.tmp_price where transaction_id = %s;", (id,))
        with open(fileName, 'r') as f:
            cur.copy_expert(
                "COPY dl.tmp_price(" +
                "transaction_id," +
                "client_code," +
                "name," +
                "articul," +
                "category_name," +
                "brand_name," +
                "default_price," +
                "company_id," +
                "company_name," +
                "region_name," +
                "price," +
                "in_stock," +
                "discount_type," +
                "discount," +
                "offer_name," +
                "date," +
                "last_check_date," +
                "original_price,"
                "date_create) " +
                "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', HEADER FALSE, QUOTE E'\\b')",
                f)
        conn.commit()
    finally:
        conn.close()


def extract_prices(id: int, writer, token: str) -> str:
    r"""
    id - идентификатор
    writer - объект записи
    token - токен
    Extract prices from external service
    """
    url = f"https://api.priceva.com/api/v1/product/list"
    headers = {"Apikey": f"{token}"}
    limit = 100
    page = 1
    date_create = datetime.now()
    index = 0
    while True:
        params = {
            "params": {
                "filters": {
                    "page": page,
                    "limit": limit,
                    "active": 1
                },
                "sources": {
                    "add": "true",
                    "add_term": "true"
                }
            }
        }
        req = httpclient.post(url=url, headers=headers, json=params)
        req.raise_for_status()
        data = req.json()
        result = data["result"]
        pagination = result["pagination"]
        pages_cnt = pagination["pages_cnt"]
        max_page = pagination["page"]
        objects = result["objects"]
        size = len(objects)
        logging.info(f"Size: {size}")
        if size == 0:
            break
        for obj in objects:
            client_code = obj["client_code"]
            name = obj["name"]
            articul = obj["articul"]
            category_name = obj["category_name"]
            brand_name = obj["brand_name"]
            default_price = obj["default_price"]
            sources = obj["sources"]
            for source in sources:
                company_id = source["company_id"]
                if company_id == False:
                    continue
                company_name = source["company_name"]
                region_name = source["region_name"]
                offers = source.get("offers")
                if offers:
                    for offer in offers:
                        price = offer["price"]
                        in_stock = offer["in_stock"] == 1
                        discount_type = offer["discount_type"]
                        discount = offer["discount"]
                        offer_name = offer["offer"]
                        date = datetime.fromtimestamp(offer["date"])
                        last_check_date = datetime.fromtimestamp(offer["last_check_date"])
                        if discount_type == 0:
                            original_price = price * (1 + discount / 100)
                        else:
                            original_price = price + discount
                        # product = Product(barcode=client_code, source=company_id, offer=offer_name)
                        # if product in products_map:
                        #     product_id = products_map.get(product)
                        #     continue
                        # product_id = -1
                        index += 1
                        write(writer, id, client_code, name, articul, category_name, brand_name, default_price,
                              company_id, company_name, region_name, price, in_stock, discount_type, discount,
                              offer_name,
                              date, last_check_date, original_price, date_create)
                else:
                    price = source.get("price")
                    if price is None:
                        continue
                    in_stock = source["in_stock"] == 1
                    discount_type = source["discount_type"]
                    discount = source["discount"]
                    offer_name = company_name
                    date = datetime.fromtimestamp(offer["date"])
                    last_check_date = datetime.fromtimestamp(offer["last_check_date"])
                    if discount_type == 0:
                        original_price = price * (1 + discount / 100)
                    else:
                        original_price = price + discount
                    # product = Product(barcode=client_code, source=company_id, offer=offer_name)
                    # if product in products_map:
                    #     product_id = products_map.get(product)
                    #     continue
                    # product_id = -1
                    index += 1
                    write(writer, id, client_code, name, articul, category_name, brand_name, default_price,
                          company_id, company_name, region_name, price, in_stock, discount_type, discount, offer_name,
                          date, last_check_date, original_price, date_create)
        logging.info(f"In iteration: {index}")
        if max_page >= pages_cnt:
            logging.info(f"Finished {index}")
            break
        page += 1
    pass
