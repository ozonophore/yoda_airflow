import logging
from datetime import date, timedelta, datetime
from time import sleep

from dags import httpclient


def wb_extract_orders(id: int, writer, owner: str, token: str, dateFrom: date, dateTo: date,
                      time_out_in_sec: int = 20) -> None:
    currentDate = dateFrom
    maxDate = datetime(year=dateFrom.year, month=dateFrom.month, day=dateFrom.day)
    count = 0
    while True:
        if currentDate > dateTo:
            break
        logging.info(f"Extracting orders by {maxDate}")
        currentDateStr = maxDate.strftime('%Y-%m-%dT%H:%M:%S')
        flag = 0
        url = f'http://statistics-api.wildberries.ru/api/v1/supplier/orders?dateFrom={currentDateStr}&flag={flag}'
        resp = httpclient.get(url,
                              headers={"Authorization": f"Bearer {token}"})
        resp.raise_for_status()
        data = resp.json()
        size = len(data)
        logging.info(f"Size of orders: {size}")
        if size == 0:
            break
        for row in data:
            if datetime.strptime(row['date'], '%Y-%m-%dT%H:%M:%S').date() < dateFrom:
                continue  # skip old data
            maxDate = max(maxDate, datetime.strptime(row['lastChangeDate'], '%Y-%m-%dT%H:%M:%S'))
            writer.writerow(
                [row['date'],
                 owner,
                 row['lastChangeDate'],
                 row['supplierArticle'],
                 row['techSize'],
                 row['barcode'],
                 row['totalPrice'],
                 row['discountPercent'],
                 str(row['warehouseName']).upper(),
                 row['regionName'],
                 row['incomeID'],
                 0,
                 row['subject'],
                 row['category'],
                 row['brand'],
                 row['isCancel'],
                 row['cancelDate'],
                 row['gNumber'],
                 row['sticker'],
                 row['srid'],
                 row['orderType'],
                 row['nmId'],
                 row['spp'],
                 row['finishedPrice'],
                 row['priceWithDisc'],
                 row['countryName'],
                 row['oblastOkrugName'],
                 row['regionName'],
                 id
                 ])
            count += 1
        logging.info(f"Inserted {count} rows")
        #currentDate += timedelta(days=1)
        maxDate += timedelta(seconds=1)
        logging.info(f"Waiting {time_out_in_sec} sec")
        sleep(time_out_in_sec)
