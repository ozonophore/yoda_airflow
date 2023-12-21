import logging
from datetime import date, datetime, timedelta
from time import sleep

from dags import httpclient


def wb_extract_sales(id: int, writer, owner: str, token: str, date_from: date,
                     date_to: date, time_out_in_sec: int = 20) -> set:
    r"""
    Extract sales
    :param id: ид транзакции
    :param writer:
    :param owner: код кабинета
    :param token: токен
    :param date_from: дата начала периода
    :param date_to: дата окончания периода
    :param time_out_in_sec: how long to wait
    return - список кодов складов
    """
    logging.info(f"Extracting sales {owner} from {date_from} to {date_to}")
    currentDate = date_from
    warehouses = set()
    maxDate = datetime(date_from.year, date_from.month, date_from.day)
    count = 0
    while True:
        if maxDate.date() > date_to:
            break
        currentDateStr = maxDate.strftime('%Y-%m-%dT%H:%M:%S')
        logging.info(f"Extracting sales {maxDate}")
        flag = 0
        resp = httpclient.get(
            url=f'https://statistics-api.wildberries.ru/api/v1/supplier/sales?dateFrom={currentDateStr}&flag={flag}',
            headers={"Authorization": f"Bearer {token}"})
        resp.raise_for_status()
        data = resp.json()
        size = len(data)
        logging.info(f"Sales size: {size}")
        if size == 0:
            break
        for row in data:
            if row['srid'] == 0:
                continue
            maxDate = max(maxDate, datetime.strptime(row['lastChangeDate'], '%Y-%m-%dT%H:%M:%S'))
            row_date = datetime.strptime(row['date'], '%Y-%m-%dT%H:%M:%S')
            if row_date.date() < date_from:
                continue  # skip old data
            wh = str(row['warehouseName']).upper()
            warehouses.add(wh)
            writer.writerow([
                row['date'],
                owner,
                row['lastChangeDate'],
                wh,
                row['countryName'],
                row['oblastOkrugName'],
                row['regionName'],
                row['supplierArticle'],
                row['barcode'],
                row['category'],
                row['subject'],
                row['brand'],
                row['techSize'],
                row['incomeID'],
                row['isSupply'],
                row['isRealization'],
                row['totalPrice'],
                row['discountPercent'],
                row['spp'],
                row['forPay'],
                row['finishedPrice'],
                row['priceWithDisc'],
                row['saleID'],
                row['sticker'],
                row['gNumber'],
                0,
                row['srid'],
                row['nmId'],
                id
            ])
            count += 1
        logging.info(f"Sales have written {count} rows")

        maxDate += timedelta(seconds=1)
        logging.info(f"Waiting {time_out_in_sec} sec")
        sleep(time_out_in_sec)
    return warehouses