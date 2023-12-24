import logging
from datetime import datetime

from dags import httpclient


def extract_stock(id: int, owner: str, writer, token: str, stockDate: datetime.date,
                     dateFrom: datetime.date) -> None:
    r"""
    Получение остатков
        id - идентификатор
        owner - код организации
        writer - объект для записи данных
        token - токен
        stockDate - дата остатков
        dateFrom - дата начала периода c которой нужно получить остатки
    """
    dateFromStr = dateFrom.strftime('%Y-%m-%d')
    req = httpclient.get(f"https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={dateFromStr}",
                         headers={"Authorization": f"Bearer {token}"})
    items = req.json()
    if len(items) == 0:
        logging.info("No data")
        return None

    for item in items:
        writer.writerow(
            [id,
             owner,
             item['lastChangeDate'],
             str(item['warehouseName']).upper(),
             item['supplierArticle'],
             item['barcode'],
             item['quantity'],
             item['inWayToClient'],
             item['inWayFromClient'],
             item['quantityFull'],
             item['category'],
             item['subject'],
             item['brand'],
             item['Price'],
             item['Discount'],
             item['isSupply'],
             item['isRealization'],
             item['nmId'],
             stockDate])
