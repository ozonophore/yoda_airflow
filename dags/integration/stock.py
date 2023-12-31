import datetime
import logging

from dags import httpclient


def extract_stock_data(id: int, writer, host: str, token: str, date: datetime.date) -> None:
    r"""
    Extract stock data from 1C
    """
    url = f"{host}/stocks"
    headers = {"Key": f"{token}"}
    req = httpclient.get(url=url, headers=headers)
    req.raise_for_status()
    data = req.json()
    items = data["items"]
    size = len(items)
    if size == 0:
        logging.info(f"Data is empty, skipping {date}")
        return None
    index = 0
    for item in items:
        item_id = item["id"]
        if not bool(item_id and item_id.strip()):
            logging.info(f"Skipping row: {index}")
            continue
        writer.writerow([
            id, date, item["id"], item["quantity"]
        ])
        index += 1
    logging.info(f"Finished {index}")

