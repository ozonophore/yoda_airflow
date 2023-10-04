import sys

from airflow.exceptions import AirflowSkipException
from pendulum import duration

sys.path.append('/opt/airflow/dags/statistics/common')
from common import get_secret_token, write_status

import logging
import re
from datetime import datetime

import requests
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dateutil.relativedelta import relativedelta

default_args = {
    'host': 'https://performance.ozon.ru{0}',
    'url': 'https://performance.ozon.ru/api/client/token',
    'statistic_url': 'https://performance.ozon.ru/api/client/vendors/statistics',
    'check_url': 'https://performance.ozon.ru:443/api/client/vendors/statistics/{0}?vendor=true',
    'report_days': 60,
}

def prepare_report(token, dataType):
    logging.info("Token: %s", token)
    url = default_args["statistic_url"]
    logging.info("URL: %s", url)
    headers = {"Content-Type": "application/json", "Accept": "application/json",
               "Authorization": "Bearer " + token}
    offDays = default_args["report_days"]
    data = {"dateFrom": (datetime.today()  - relativedelta(days=offDays)).strftime('%Y-%m-%d'),
            "dateTo": datetime.today().strftime('%Y-%m-%d'),
            "type": dataType}
    logging.info("Data: %s", data)
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        raise Exception("Error getting token ", response.status_code, response.text)
    logging.info("Response: %s", response.json()['UUID'])
    uuid = response.json()['UUID']
    return uuid


with DAG(
        dag_id="performance_request",
        description="Запуск формирования отчёта с аналитикой внешнего трафика.",
        schedule="@daily",
        start_date=datetime(2023, 1, 1, 23),
        catchup=False,
        tags=['statistic', 'ozon'],
        default_args={
            "retries": 3,
            "retry_delay": duration(seconds=30),
        },
) as dag:
    dag.doc_md = __doc__


    def get_connections():
        sql_stmt = "select owner_code, source, client_id, token from ml.statistic_marketplase"
        pg_hook = PostgresHook(
            postgres_conn_id='database'
        )
        return pg_hook.get_records(sql_stmt)


    #
    for row in get_connections():
        @task(task_id="get_secret_token_" + row[0].lower())
        def get_secret_token_task(clientId, clientSecret):
            url = default_args["url"]
            return get_secret_token(clientId, clientSecret, url)


        @task(task_id="prepare_report_traffic_" + row[0].lower())
        def prepare_report_traffic_task(access_token):
            return prepare_report(access_token, "TRAFFIC_SOURCES")


        @task(task_id="prepare_report_orders_" + row[0].lower())
        def prepare_report_orders_task(access_token):
            return prepare_report(access_token, "ORDERS")


        @task(task_id="write_status_ts_" + row[0].lower())
        def write_status_ts_task(uuid, client):
            write_status(uuid, client, 'TRAFFIC_SOURCES')


        @task(task_id="write_status_o_" + row[0].lower())
        def write_status_o_task(uuid, client):
            write_status(uuid, client, 'ORDERS')


        access_token = get_secret_token_task(row[2], row[3])
        write_status_ts_task(prepare_report_traffic_task(access_token), row[0])
        write_status_o_task(prepare_report_orders_task(access_token), row[0])


with DAG(
        dag_id="performance_ozon_check_status",
        description="Проверка статуса заявки и выгрузка файла.",
        schedule="*/10 * * * *",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['statistic', 'ozon'],
) as dag:
     dag.doc_md = __doc__

     @task(task_id="get_connections")
     def get_connections():
         sql_stmt = "select owner_code, source, client_id, token from ml.statistic_marketplase"
         pg_hook = PostgresHook(
             postgres_conn_id='database'
         )
         result = dict()
         for row in pg_hook.get_records(sql_stmt):
            result[row[0]] = {'client_id': row[2], 'token': row[3]}
         return result

     @task(task_id="get_uuid")
     def get_uuid():
         sql_stmt = ("select uuid, owner_code, status from ml.statistic_request where status in('NOT_STARTED', 'IN_PROGRESS')")
         pg_hook = PostgresHook(
             postgres_conn_id='database'
         )
         records = pg_hook.get_records(sql_stmt)
         if len(records) == 0:
             raise AirflowSkipException("No records to process")
         return records

     def download_file(uuid, link, token):
         host = default_args["host"]
         resp = requests.get( host.format(link), headers={"Authorization": "Bearer " + token})
         if resp.status_code != 200:
            raise Exception("Error getting response ", resp.status_code, resp.text)
         logging.info("Response: %s", resp.headers)
         fname = re.findall('filename="(.+)"', resp.headers['content-disposition'])[0]
         logging.info("Origin FileName: %s", fname)
         fileName = "data/" + uuid + "." + fname
         with open(fileName, 'wb') as f:
             s = f.write(resp.content)
         logging.info("FileName: %s", fileName)
         return s

     @task(task_id="download_files")
     def download_files(items):
         logging.info("Items: %s", items)
         for item in items:
             logging.info("Item: %s", item)
             uuid = item['uuid']
             link = item['link']
             access_token = item['access_token']
             r = download_file(uuid, link, access_token)
             logging.info("File status: %s", r)
         pass


     @task(task_id="check_and_update")
     def check_and_update(docs, connections):
         url = default_args["url"]
         tokens = dict()
         to_download = list()
         for doc in docs:
             uuid = doc[0]
             owner_code = doc[1]
             status = doc[2]
             logging.info("UUID: %s status: %s", uuid, status)
             client_id = connections[owner_code]['client_id']
             token = connections[owner_code]['token']
             if owner_code not in tokens:
                 tokens[owner_code] = get_secret_token(client_id, token, url)
             access_token = tokens[owner_code]
             check_url = default_args["check_url"].format(uuid)
             headers = {"Content-Type": "application/json", "Accept": "application/json",
                        "Authorization": "Bearer " + access_token}
             resp = requests.get(check_url, headers=headers)
             if resp.status_code != 200:
                 raise Exception("Error getting response ", resp.status_code, resp.text)
             logging.info("Response: %s", resp.json())
             new_status = resp.json()['state']
             logging.info("UUID: %s New status: %s", uuid, new_status)
             if new_status != status:
                link = resp.json().get('link')
                error = resp.json().get('error')
                sql_stmt = "update ml.statistic_request set status=%s, link=%s, error=%s where uuid=%s"
                pg_hook = PostgresHook(
                        postgres_conn_id='database'
                    )
                pg_hook.run(sql_stmt, parameters=(new_status, link, error, uuid))
                if new_status == 'OK':
                    to_download.append({"uuid": uuid, "link": link, "access_token": access_token})
         return to_download

     docs = check_and_update(get_uuid(), get_connections())
     download_files(docs)
