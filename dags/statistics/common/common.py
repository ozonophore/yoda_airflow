import logging

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook


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