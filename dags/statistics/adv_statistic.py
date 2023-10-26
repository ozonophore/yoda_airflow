import logging
import re
import sys
from datetime import datetime, timedelta, date
from time import sleep

import requests
import xlrd
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.settings import json

sys.path.append('/opt/airflow')
from dags.statistics.common.common import write_status, set_new_status, set_loaded_status, set_loaded_error_status, \
    RowValueTraffic, RowValue

TYPES = ['TRAFFIC_SOURCES', 'ORDERS']

DEFAULT_ARGS = {
    'off_days': 60,
    'work_dir': 'data/',
    'retries': 3,
}


def get_doc_uuid(token, dataType, dateFrom, dateTo, url):
    logging.info("Token: %s", token)
    logging.info("URL: %s", url)
    headers = {"Content-Type": "application/json", "Accept": "application/json",
               "Authorization": "Bearer " + token}
    data = {"dateFrom": dateFrom.strftime('%Y-%m-%d'),
            "dateTo": dateTo.strftime('%Y-%m-%d'),
            "type": dataType}
    logging.info("Data: %s", data)
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        raise Exception("Error getting token ", response.status_code, response.text)
    logging.info("Response: %s", response.json()['UUID'])
    uuid = response.json()['UUID']
    return uuid


def prepare_report_task(access_token, type_report, off_days, dateFromStr, url) -> str:
    logging.info(dag.params)
    dateTo = datetime.strptime(dateFromStr, '%Y-%m-%d').date()
    dateFrom = (dateTo - timedelta(days=off_days))
    return get_doc_uuid(access_token, type_report, dateFrom, dateTo, url)


def check_status(uuid, access_token, url):
    headers = {"Content-Type": "application/json", "Accept": "application/json",
               "Authorization": "Bearer " + access_token}
    status = ['NOT_STARTED', 'IN_PROGRESS']
    while True:
        resp = requests.get(url, headers=headers)
        logging.info("Response: %s", resp.text)
        if resp.status_code != 200:
            logging.info("Response status: %s msg: %s", resp.status_code, resp.text)
            sleep(20)
            continue
        new_status = resp.json()['state']
        error = resp.json().get('error')
        logging.info("UUID: %s New status: %s error: %s", uuid, new_status, error)
        link = resp.json().get('link')
        set_new_status(error, link, new_status, uuid)
        if new_status in status:
            sleep(20)
            continue
        if new_status not in status and new_status != 'OK':
            yield False
        yield new_status == 'OK'
        yield link


def req_resp_wait(owner_code, type_report, access_token, off_days, dateFromStr) -> str:
    logging.info("Owner code: %s", owner_code)
    logging.info("Type: %s", type_report)
    logging.info("Off days: %s", off_days)
    while True:
        host = BaseHook.get_connection("http_ozon_statistic").host
        uuid = prepare_report_task(access_token, type_report, int(off_days), dateFromStr, host + '/api/client/vendors'
                                                                                                 '/statistics')
        write_status(uuid, owner_code, type_report)
        context = get_current_context()
        context['ti'].xcom_push(key='uuid_{0}_{1}'.format(owner_code, type_report), value=uuid)
        sleep(10)
        logging.info("Start check status")
        check_result = check_status(uuid, access_token,
                                    host + '/api/client/vendors/statistics/{0}?vendor=true'.format(uuid))
        if not next(check_result):
            sleep(10)
            continue
        return next(check_result)


def download_file(url, token, uuid):
    logging.info("UUID: %s", uuid)
    logging.info("Token: %s", token)

    host = BaseHook.get_connection("http_ozon_statistic").host
    url = host + url
    logging.info("URL: %s", url)
    resp = requests.get(url, headers={"Authorization": "Bearer " + token})
    if resp.status_code != 200:
        set_loaded_error_status("HTTP: {0}".format(resp.status_code), 'ERROR_LOAD', uuid)
        raise Exception("Error getting response ", resp.status_code, resp.text)
    logging.info("Response: %s", resp.headers)
    fname = re.findall('filename="(.+)"', resp.headers['content-disposition'])[0]
    logging.info("Origin FileName: %s", fname)
    working_dir = DEFAULT_ARGS["work_dir"]
    fileName = uuid + "." + fname
    try:
        with open(working_dir + fileName, 'wb') as f:
            s = f.write(resp.content)
        logging.info("FileName: %s size: %d", fileName, s)
        set_loaded_status(None, 'LOADED', uuid, fileName)
    except Exception as e:
        set_loaded_error_status(e, 'ERROR_LOAD', uuid)
        raise Exception("Error write file ", e)
    return fileName


def parse_excel_file(file, type):
    working_dir = DEFAULT_ARGS["work_dir"]
    file = working_dir + file
    print("Finish parse file: ", file)
    logging.info("File: %s", file)
    wb = xlrd.open_workbook(file)
    worksheet = wb.sheet_by_index(0)
    rows = worksheet.get_rows()
    logging.info("Type: %s", type)
    # skip header
    next(rows)
    data = list()
    for row in rows:
        logging.info("Row: %s", row)
        if type == 'TRAFFIC_SOURCES':
            value = RowValueTraffic(row)
        else:
            value = RowValue(row)
        data.append(value)
    return data


def prepare_data(type_report, uuid, file_name, owner_code):
    data = parse_excel_file(file_name, type_report)
    sql_queries = list()
    if type_report == 'TRAFFIC_SOURCES':
        sql = "INSERT INTO dl.adv_statistic_traffic(doc_uuid,owner_code,date,source,medium,campaign,content,term,sessions,followers,add_to_basket,add_to_favorites,cancel,session_duration,ordered_qnt,sum_cost,sum_price,range_attr_qnt,range_attr_sum_cost,range_attr_sum_price) VALUES "
    else:
        sql = "INSERT INTO dl.adv_statistic_orders(doc_uuid,owner_code,date, id_com_category, category_name, sku, name, seller_id, seller_name, brand_id, brand_name, source, medium, campaign, content, term, ordered_qnt, sum_price, sum_cost, range_attr_qnt, range_attr_sum_price, range_attr_sum_cost) VALUES "

    for item in data:
        if type_report == 'TRAFFIC_SOURCES':
            sql_query = (f"('{uuid}','{owner_code}', {item})")
        else:
            sql_query = (f"('{uuid}','{owner_code}',{item})")
        sql_queries.append(sql_query)
    pg_hook = PostgresHook(
        postgres_conn_id='database'
    )
    if len(sql_queries) > 0:
        pg_hook.run('{0} {1}'.format(sql, ','.join(sql_query)))
        set_loaded_error_status(None, 'UPLOADED', uuid)


with DAG(
        dag_id="adv_statistic",
        schedule="30 1 * * *",
        start_date=datetime(2023, 10, 11, 1),
        max_active_runs=1,
        catchup=False,
        default_args=DEFAULT_ARGS,
        params={
            "report_date": Param((date.today() - timedelta(days=1)).strftime("%Y-%m-%d"), title="Дата", format="date",
                                 type="string",
                                 description="Дата формирования отчёта"),
            "off_days": Param(DEFAULT_ARGS.get('off_days'), title="Дней", type="integer", format="integer",)
        },
) as dag:
    start = EmptyOperator(task_id="start")
    clean = PostgresOperator(
        task_id="clean",
        postgres_conn_id="database",
        sql='sql/adv_statistic_clean.sql',
    )
    commit = PostgresOperator(
        task_id="commit",
        postgres_conn_id="database",
        sql='sql/adv_statistic_commit.sql',
        params={
            "date": datetime.strptime(dag.params['report_date'], '%Y-%m-%d').date() - timedelta(days=dag.params['off_days']),
        },
    )

    orgs = PostgresHook(postgres_conn_id="database").get_records(
        "SELECT owner_code, client_id, token FROM ml.statistic_marketplase t")

    for org in orgs:
        get_token = SimpleHttpOperator(
            task_id=str(org[0]).lower() + "_get_token",
            http_conn_id="http_ozon_statistic",
            endpoint="api/client/token",
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            # check_response=lambda response: True if len(response.json()) == 0 else False,
            response_filter=lambda response: response.json()["access_token"],
            data=json.dumps({"client_id": org[1], "client_secret": org[2], "grant_type": "client_credentials"}),
        )

        for typeReport in TYPES:
            req_resp_wait_task = PythonOperator(
                task_id="{0}_{1}_req_resp_wait".format(str(org[0]).lower(), typeReport.lower()),
                python_callable=req_resp_wait,
                op_kwargs={
                    "owner_code": str(org[0]),
                    "type_report": typeReport,
                    "access_token": '{{ task_instance.xcom_pull(task_ids="' + str(
                        org[0]).lower() + '_get_token' + '") }}',
                    "off_days": '{{ params.off_days }}',
                    "dateFromStr": '{{ params.report_date }}'
                },
                execution_timeout=timedelta(minutes=5),
            )

            download = PythonOperator(task_id="{0}_{1}_download".format(str(org[0]).lower(), typeReport.lower()),
                                      python_callable=download_file,
                                      op_kwargs={
                                          "url": "{{ task_instance.xcom_pull(task_ids='" + str(
                                              org[
                                                  0]).lower() + "_" + typeReport.lower() + "_req_resp_wait" + "', key='return_value') }}",
                                          "uuid": "{{ task_instance.xcom_pull(key='" + 'uuid_{0}_{1}'.format(org[0],
                                                                                                             typeReport)
                                                  + "') }}",
                                          "token": '{{ task_instance.xcom_pull(task_ids="' + str(
                                              org[0]).lower() + '_get_token' + '") }}',
                                      },
                                      )

            parse_and_store = PythonOperator(
                task_id="{0}_{1}_parse_and_store".format(str(org[0]).lower(), typeReport.lower()),
                python_callable=prepare_data,
                op_kwargs={
                    "type_report": typeReport,
                    "owner_code": str(org[0]),
                    "uuid": "{{ task_instance.xcom_pull(key='" + 'uuid_{0}_{1}'.format(org[0], typeReport)
                            + "') }}",
                    "file_name": "{{ task_instance.xcom_pull(task_ids='" + str(
                        org[
                            0]).lower() + "_" + typeReport.lower() + "_download" + "', key='return_value') }}"
                }
            )

            delete_file = BashOperator(
                task_id="{0}_{1}_delete_file".format(str(org[0]).lower(), typeReport.lower()),
                cwd=DEFAULT_ARGS['work_dir'],
                bash_command="rm -f {{ task_instance.xcom_pull(task_ids='" + str(
                    org[
                        0]).lower() + "_" + typeReport.lower() + "_download" + "', key='return_value') }}"
            )

            start >> clean >> get_token >> req_resp_wait_task >> download >> parse_and_store >> delete_file >> commit
