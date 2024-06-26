import csv
import logging
import uuid
from datetime import datetime

import psycopg2

from dags import httpclient

header = ["transaction_id", "client_code", "name", "articul", "category_name", "brand_name", "default_price", "company_id",
          "company_name", "region_name", "price", "in_stock", "discount_type", "discount", "offer_name", "date",
          "last_check_date", "original_price", "date_create", "card_price"]

header_dl = ["transaction_id", "client_code", "name", "articul", "category_name", "brand_name", "default_price", "company_id",
             "company_name", "region_name", "price", "in_stock", "discount_type", "discount", "offer_name", "offer_id",
             "date",
             "last_check_date", "original_price", "date_create", "card_price"]


class Product:
    def __init__(self, barcode, source, offer):
        self.barcode = barcode
        self.source = source
        self.offer = offer

    def __hash__(self):
        return hash((self.barcode, self.source, self.offer))

    def __eq__(self, other):
        return (self.barcode, self.source, self.offer) == (other.barcode, other.source, other.offer)


__offers_dict = dict()


class OrgInfo:
    def __init__(self, code, name: str):
        self.name = name
        self.code = code

    def __hash__(self):
        return hash(self.code)

    def __eq__(self, other):
        return (self.code) == (other.code)


def __save_offer(id, key, database, user, password, host, port: str):
    conn = psycopg2.connect(database=database,
                            user=user,
                            password=password,
                            host=host,
                            port=port)
    cur = conn.cursor()
    try:
        cur.execute("insert into ml.offers(id, name) values(%s, %s)", (id, key))
        conn.commit()
        cur.close()
    finally:
        conn.close()


def __get_offer(name: str, database, user, password, host, port: str) -> str:
    r"""
    Get organization info
    """
    if not __offers_dict:
        __load_offers(database, user, password, host, port)
    key = name.strip()
    if key == "":
        return None
    id = __offers_dict.get(key)
    if id is None:
        id = uuid.uuid4()
        __save_offer(id, key, database, user, password, host, port)
        __offers_dict[key] = id
    return id


def __load_offers(database, user, password, host, port: str):
    conn = psycopg2.connect(database=database,
                            user=user,
                            password=password,
                            host=host,
                            port=port)
    cur = conn.cursor()
    try:
        cur.execute("select id, name from ml.offers")
        rows = cur.fetchall()
        for row in rows:
            id = row[0]
            name = row[1]
            __offers_dict[name] = id
    finally:
        conn.close()


def write(writer, id, client_code, name, articul, category_name, brand_name, default_price, company_id, company_name,
          region_name, price, in_stock, discount_type, discount, offer_name, date, last_check_date, original_price,
          date_create, card_price=None):
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
        date_create,
        card_price
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
                "offer_id," +
                "date," +
                "last_check_date," +
                "original_price," +
                "date_create, card_price) " +
                "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', HEADER TRUE, QUOTE E'\\b')",
                f)
        conn.commit()
    finally:
        conn.close()

class EmptyResultError(Exception):
    pass

def extract_prices(id: int, file_name, token: str) -> None:
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
    with open(file_name, 'w') as f:
        writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)
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
            if not result:
                raise EmptyResultError(f"Error in response: {data}")

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
                sources = obj.get("sources")
                if not sources:
                    logging.info(f"Sources is empty for {client_code}")
                    continue
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
                            data = offer.get("data")
                            card_price: int = None
                            if data:
                                # Находим массив объектов с ключем "card_price"
                                card_price_objects = [item for item in data if item.get("key") == "card_price"]

                                # Если найден объект с ключем "card_price", извлекаем поле "value"
                                if card_price_objects:
                                    card_price = card_price_objects[0]["value"]
                            index += 1
                            write(writer, id, client_code, name, articul, category_name, brand_name, default_price,
                                  company_id, company_name, region_name, price, in_stock, discount_type, discount,
                                  offer_name,
                                  date, last_check_date, original_price, date_create, card_price)
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
                              company_id, company_name, region_name, price, in_stock, discount_type, discount,
                              offer_name,
                              date, last_check_date, original_price, date_create)
            logging.info(f"In iteration: {index}")
            if max_page >= pages_cnt:
                logging.info(f"Finished {index}")
                break
            page += 1


def transform_prices(file_name, database, user, password, host, port: str) -> str:
    output_file = f"{file_name}.trns"
    with open(file_name, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        with open(output_file, 'w') as output_f:
            writer = csv.DictWriter(output_f, delimiter='\t', quoting=csv.QUOTE_MINIMAL, fieldnames=header_dl)
            writer.writeheader()
            logging.info(f"Start transforming {file_name}")
            for row in reader:
                offer_id = __get_offer(row["offer_name"], database, user, password, host, port)
                row["offer_id"] = offer_id
                writer.writerow(row)

    return output_file
