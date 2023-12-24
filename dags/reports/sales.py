import csv
import logging
from datetime import timedelta, datetime
from time import sleep

import requests
import urllib3
from airflow.decorators import task
from airflow.models import Param
from airflow.models.dag import dag
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=60),
    'work_dir': './data/',
    'conn_id': 'database',
}

max_retries = 10  # Максимальное количество попыток
retry_delay = 30  # Задержка в секундах между попытками

http = urllib3.PoolManager()


def request_repeater(method, url, headers, json):
    for _ in range(max_retries):
        try:
            resp = requests.request(method=method, url=url, headers=headers, json=json)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logging.error(f"Попытка: {max_retries - _} Ошибка: {e}")
        if _ < max_retries - 1:
            logging.info(f"Повторная попытка через {retry_delay} секунд...")
            sleep(retry_delay)
    raise Exception(f"Не удалось выполнить запрос после {max_retries} попыток.")


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 11, 18),
    catchup=False,
    description="Отчеты о реализации товара",
    params={"date": Param(datetime.now().strftime('%Y-%m'), title="Дата", type="string",
                          description="Дата в формате YYYY-MM", ), },
)
def report_sales():
    @task()
    def extract_order_report(owner, clientId, token):
        date = get_current_context()['params']['date']
        logging.info(f"Выгрузка отчета о реализации за {date} для {owner}")
        request = {
            "date": date
        }
        logging.info(f"Запрос: {request}")
        url = 'https://api-seller.ozon.ru/v1/finance/realization'
        resp = request_repeater('POST', url, headers={
            "Client-Id": clientId,
            "Api-Key": token,
            "Content-Type": "application/json",
        }, json=request)
        result = resp['result']
        csvfile = open(f'{default_args["work_dir"]}{owner.lower()}.ozon.sales.report.csv', 'w')
        try:
            spamwriter = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_ALL)

            spamwriter.writerow([])
            spamwriter.writerow([
                'Номер отчёта о реализации.',
                'Дата формирования отчёта.',
                'Дата заключения договора оферты.',
                'Номер договора оферты.',
                'Валюта ваших цен.',
                'Сумма к начислению.',
                'Сумма к начислению с НДС.',
                'ИНН плательщика.',
                'КПП плательщика.',
                'Название плательщика.',
                'ИНН получателя.',
                'КПП получателя.',
                'Название получателя.',
                'Начало периода в отчёте.',
                'Конец периода в отчёте.',
            ])
            header = result['header']
            spamwriter.writerow([
                header['num'],
                header['doc_date'],
                header['contract_date'],
                header['contract_num'],
                header['currency_code'],
                header['doc_amount'],
                header['vat_amount'],
                header['payer_inn'],
                header['payer_kpp'],
                header['payer_name'],
                header['rcv_inn'],
                header['rcv_kpp'],
                header['rcv_name'],
                header['start_date'],
                header['stop_date'],
            ])

            spamwriter.writerow([])
            spamwriter.writerow([
                'Номер строки в отчёте.',
                'Идентификатор товара.',
                'Наименование товара.',
                'Штрихкод товара.',
                'Код товара продавца — артикул.',
                'Комиссия за продажу по категории.',
                'Цена продавца с учётом его скидки.',
                'Цена реализации',
                'Реализовано на сумму.',
                'Комиссия за реализованный товар с учётом скидок и наценки.',
                'Доплата за счёт Ozon.',
                'Итого к начислению за реализованный товар.',
                'Количество товара',
                'Цена реализации',
                'Возвращено на сумму.',
                'Комиссия с учётом количества товара',
                'Доплата за счёт Ozon.',
                'Итого возвращено.',
                'Количество возвращённого товара.',
            ])
            for row in result['rows']:
                spamwriter.writerow([
                    row['row_number'],
                    row['product_id'],
                    row['product_name'],
                    row['barcode'],
                    row['offer_id'],
                    row['commission_percent'],
                    row['price'],
                    row['price_sale'],
                    row['sale_amount'],
                    row['sale_commission'],
                    row['sale_discount'],
                    row['sale_price_seller'],
                    row['sale_qty'],
                    row['return_sale'],
                    row['return_amount'],
                    row['return_commission'],
                    row['return_discount'],
                    row['return_price_seller'],
                    row['return_qty'],
                ])
        finally:
            csvfile.close()

    @task()
    def wb_wxtract_sales_report(owner, token):
        date = get_current_context()['params']['date']
        firstDay = datetime.strptime(date, '%Y-%m')
        lastDay = firstDay.replace(day=1, month=firstDay.month + 1) - timedelta(days=1)
        sFirstDay = firstDay.strftime('%Y-%m-%d')
        sLastDay = lastDay.strftime('%Y-%m-%d')
        logging.info(f"Выгрузка отчета о реализации за {sFirstDay} - {sLastDay} для {owner}")
        rrdid = 0
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        csvfile = None
        spamwriter = None
        fields = {
            "realizationreport_id": "Номер отчёта.",
            "date_from": "Дата начала отчётного периода",
            "date_to": "Дата конца отчётного периода",
            "create_dt": "Дата формирования отчёта",
            "currency_name": "Валюта отчёта",
            "suppliercontract_code": "Договор",
            "rrd_id": "Номер строки",
            "gi_id": "Номер поставки",
            "subject_name": "Предмет",
            "nm_id": "Артикул WB",
            "brand_name": "Бренд",
            "sa_name": "Артикул продавца",
            "ts_name": "Размер",
            "barcode": "Баркод",
            "doc_type_name": "Тип документа",
            "quantity": "Количество",
            "retail_price": "Цена розничная",
            "retail_amount": "Сумма продаж (возвратов)",
            "sale_percent": "Согласованная скидка",
            "commission_percent": "Процент комиссии",
            "office_name": "Склад",
            "supplier_oper_name": "Обоснование для оплаты",
            "order_dt": "Дата заказа.",
            "sale_dt": "Дата продажи.",
            "rr_dt": "Дата операции.",
            "shk_id": "Штрих-код",
            "retail_price_withdisc_rub": "Цена розничная с учетом согласованной скидки",
            "delivery_amount": "Количество доставок",
            "return_amount": "Количество возвратов",
            "delivery_rub": "Стоимость логистики",
            "gi_box_type_name": "Тип коробов",
            "product_discount_for_report": "Согласованный продуктовый дисконт",
            "supplier_promo": "Промокод",
            "rid": "Уникальный идентификатор заказа",
            "ppvz_spp_prc": "Скидка постоянного покупателя",
            "ppvz_kvw_prc_base": "Размер кВВ без НДС, % базовый",
            "ppvz_kvw_prc": "Итоговый кВВ без НДС, %",
            "sup_rating_prc_up": "Размер снижения кВВ из-за рейтинга",
            "is_kgvp_v2": "Размер снижения кВВ из-за акции",
            "ppvz_sales_commission": "Вознаграждение с продаж до вычета услуг поверенного, без НДС",
            "ppvz_for_pay": "К перечислению продавцу за реализованный товар",
            "ppvz_reward": "Возмещение за выдачу и возврат товаров на ПВЗ",
            "acquiring_fee": "Возмещение издержек по эквайрингу.",
            "acquiring_bank": "Наименование банка-эквайера",
            "ppvz_vw": "Вознаграждение WB без НДС",
            "ppvz_vw_nds": "НДС с вознаграждения WB",
            "ppvz_office_id": "Номер офиса",
            "ppvz_office_name": "Наименование офиса доставки",
            "ppvz_supplier_id": "Номер партнера",
            "ppvz_supplier_name": "Партнер",
            "ppvz_inn": "ИНН партнера",
            "declaration_number": "Номер таможенной декларации",
            "bonus_type_name": "Обоснование штрафов и доплат.",
            "sticker_id": "Цифровое значение стикера",
            "site_country": "Страна продажи",
            "penalty": "Штрафы",
            "additional_payment": "Доплаты",
            "rebill_logistic_cost": "Возмещение издержек по перевозке.",
            "rebill_logistic_org": "Организатор перевозки.",
            "kiz": "Код маркировки.",
            "srid": "Уникальный идентификатор заказа.",
        }

        while True:
            request = f"dateFrom={sFirstDay}&dateTo={sLastDay}&limit=40000&rrdid={rrdid}"
            resp = request_repeater('GET',
                                    f'https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod?{request}',
                                    headers=headers, json=None)
            if resp is None:
                logging.info("Выгрузка завершена.")
                break
            size = len(resp)
            if size == 0:
                break
            if csvfile is None:
                csvfile = open(f'{default_args["work_dir"]}{owner.lower()}_wb_sales_report.csv', 'w', encoding='utf-8')
                spamwriter = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_ALL)
                header = []
                for key in fields.keys():
                    header.append(fields[key])
                spamwriter.writerow(header)
            for row in resp:
                rrdid = row["rrd_id"]
                values = []
                for key in fields.keys():
                    values.append(row.get(key))
                spamwriter.writerow(values)
            logging.info(f"Выгружено {size} записей.")
            csvfile.flush()
        if csvfile is not None:
            csvfile.close()

    orgs = PostgresHook(postgres_conn_id="database").get_records(
        "SELECT owner_code, password, client_id, source FROM ml.owner_marketplace t")

    for org in orgs:
        if org[3] == 'OZON':
            pass
            # extract_order_report_task = extract_order_report.override(task_id=f"{str(org[0]).lower()}_ozon_extract")(org[0], org[2], org[1])
            # extract_order_report_task
        else:
            wb_wxtract_sales_report_task = wb_wxtract_sales_report.override(
                task_id=f"{str(org[0]).lower()}_wb_extract")(org[0], org[1])


dag_run = report_sales()
