import logging
from datetime import datetime

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook


class RowValue:
    def __init__(self, row):
        self.date = datetime.strptime(row[0].value, '%Y-%m-%d').date()
        self.IdComCategory = int(row[1].value)
        self.CategoryName = row[2].value.replace("'", "''")
        skuAndName = row[3].value.split('/')
        self.SKU = skuAndName[0]
        self.Name = skuAndName[1].replace("'", "''")
        self.SellerID = int(row[4].value)
        self.SellerName = row[5].value.replace("'", "''")
        self.BrandID = int(row[6].value)
        self.BrandName = row[7].value.replace("'", "''")
        sourceMedium = row[8].value.split('/')
        self.Source = sourceMedium[0]
        self.Medium = sourceMedium[1]
        self.Campaign = row[9].value
        self.Content = row[10].value
        self.Term = row[11].value
        self.OrderedQnt = int(row[12].value)
        self.SumPrice = row[13].value
        self.SumCost = row[14].value
        self.RangeAttrQnt = int(row[15].value)
        self.RangeAttrSumPrice = row[16].value
        self.RangeAttrSumCost = row[17].value

    def __str__(self):
        if self.Content is None:
            self.Content = "null"
        else:
            self.Content = f"'{self.Content}'"
        if self.Term is None:
            self.Term = "null"
        else:
            self.Term = f"'{self.Term}'"
        return f"to_date('{self.date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD'), {self.IdComCategory}, '{self.CategoryName}', '{self.SKU}', '{self.Name}', {self.SellerID}, '{self.SellerName}', {self.BrandID}, '{self.BrandName}', '{self.Source}', '{self.Medium}', '{self.Campaign}', {self.Content}, {self.Term}, {self.OrderedQnt}, {self.SumPrice}, {self.SumCost}, {self.RangeAttrQnt}, {self.RangeAttrSumPrice}, {self.RangeAttrSumCost}"


class RowValueTraffic(RowValue):
    def __init__(self, row):
        self.date = datetime.strptime(row[0].value, '%Y-%m-%d').date()
        sourceMedium = row[1].value.split('/')
        self.Source = sourceMedium[0]
        self.Medium = sourceMedium[1]
        self.Campaign = row[2].value
        self.Content = row[3].value
        self.Term = row[4].value
        self.Sessions = int(row[5].value)
        self.Followers = int(row[6].value)
        self.AddToBasket = int(row[7].value)
        self.AddToFavorites = int(row[8].value)
        self.Cancel = int(row[9].value)
        self.SessionDuration = int(row[10].value)
        self.OrderedQnt = int(row[12].value)
        self.SumCost = row[13].value
        self.SumPrice = row[14].value
        self.RangeAttrQnt = int(row[15].value)
        self.RangeAttrSumPrice = row[17].value
        self.RangeAttrSumCost = row[16].value

    def __str__(self):
        if self.Content is None:
            self.Content = "null"
        else:
            self.Content = f"'{self.Content}'"
        if self.Term is None:
            self.Term = "null"
        else:
            self.Term = f"'{self.Term}'"
        return f"to_date('{self.date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD'), '{self.Source}', '{self.Medium}', '{self.Campaign}', {self.Content}, {self.Term}, {self.Sessions}, {self.Followers}, {self.AddToBasket}, {self.AddToFavorites}, {self.Cancel}, {self.SessionDuration}, {self.OrderedQnt}, {self.SumCost}, {self.SumPrice}, {self.RangeAttrQnt}, {self.RangeAttrSumCost}, {self.RangeAttrSumPrice}"


def get_secret_token(clientId, clientSecret, url):
    logging.info("URL: %s", url)
    logging.info("Client ID: %s", clientId)
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    data = {"client_id": clientId,
            "client_secret": clientSecret,
            "grant_type": "client_credentials"}
    logging.info("Data: %s", data)
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        raise Exception("Error getting token ", response.status_code, response.text)
    return response.json()['access_token']


def write_status(uuid, owner_code, type):
    logging.info("UUID: %s", uuid)
    logging.info("Owner code: %s", owner_code)
    logging.info("Type: %s", type)
    sql = "insert into ml.statistic_request(uuid, owner_code, source, request_date, status, date_from, date_to, type) " + \
          "values (%s, %s, 'OZON', now(), 'NOT_STARTED', now()::date - interval '3 month', now()::date, %s);"
    pg_hook = PostgresHook(
        postgres_conn_id='database'
    )
    pg_hook.run(sql, parameters=(uuid, owner_code, type))
    pass


def set_loaded_error_status(error, status, uuid):
    sql_stmt = "update ml.statistic_request set status=%s, error=%s where uuid=%s"
    pg_hook = PostgresHook(
        postgres_conn_id='database'
    )
    pg_hook.run(sql_stmt, parameters=(status, error, uuid))


def set_loaded_status(error, new_status, uuid, fileName):
    sql_stmt = "update ml.statistic_request set status=%s, error=%s, file_name= %s where uuid=%s"
    pg_hook = PostgresHook(
        postgres_conn_id='database'
    )
    pg_hook.run(sql_stmt, parameters=(new_status, error, fileName, uuid))


def set_new_status(error, link, new_status, uuid):
    sql_stmt = "update ml.statistic_request set status=%s, link=%s, error=%s where uuid=%s"
    pg_hook = PostgresHook(
        postgres_conn_id='database'
    )
    pg_hook.run(sql_stmt, parameters=(new_status, link, error, uuid))
