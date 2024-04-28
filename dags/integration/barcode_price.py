import logging
from datetime import datetime

import psycopg2

from dags import httpclient


def extract_barcode_prices(id: int, writer, host: str, token: str) -> None:
    r"""
    Extract prices - barcodes from 1C
    """
    url = f"{host}/prices"
    headers = {"Key": f"{token}"}
    req = httpclient.get(url=url, headers=headers)
    req.raise_for_status()
    data = req.json()
    items = data["Данные"]
    size = len(items)
    now = datetime.now()
    if size == 0:
        logging.info(f"Data is empty, skipping")
        return None
    exists_barcodes = set()
    for item in items:
        name = item["Номенклатура"]
        if name.lower().find("устарел") != -1 or name.lower().find("не использовать") != -1:
            continue
        rrc = item["РРЦ"]
        distr = item["Дистр"]
        net = item["Себестоимость"]
        barcodes = set(item["Штрихкоды"])
        for barcode in barcodes:
            if barcode is None or barcode.strip() == "" or barcode in exists_barcodes:
                continue
            writer.writerow([
                id, name, rrc, distr, net, barcode.strip(), now
            ])
            exists_barcodes.add(barcode)

def load_barcode_prices(fileName: str, database, user, password, host, port: str) -> None:
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
        cur.execute("delete from dl.tmp_barcode_price;")
        with open(fileName, 'r') as f:
            cur.copy_expert(
                "COPY dl.tmp_barcode_price(" +
                "transaction_id," +
                "name," +
                "rrc," +
                "distr," +
                "net," +
                "barcode," +
                "date_update) " +
                "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', HEADER FALSE, QUOTE '\"')",
                f)
        conn.commit()
    finally:
        conn.close()