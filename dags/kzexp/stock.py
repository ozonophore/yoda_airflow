import csv
import logging
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context


def extract_data(target: str, login: str, password: str, context: Context) -> None:
    r"""
    Выгрузка данных c cайта
    """
    url = "http://selenium:4444/wd/hub"
    bash_task = BashOperator(
        task_id='bash_example',
        bash_command=f'cd /opt/airflow/tools && java -jar kzex.jar {url} stocks {login} {password} {target}'
    )
    bash_task.execute(context=context)


def transform_data(id: int, source: str, writer, stock_date: datetime.date) -> None:
    r"""
    Преобразование данных
    """
    with open(source) as csvfile:
        spamreader = csv.DictReader(csvfile)
        count = 0
        for row in spamreader:
            writer.writerow([
                stock_date,
                row.get("Наименование"),
                row.get("Штрихкод"),
                row.get("SKU"),
                row.get("SKU").split('-')[0],
                row.get("ID товара"),
                row.get("К отправке"),
                row.get("В продаже"),
                row.get("Возврат"),
                row.get("Брак"),
                float(row.get("Себест. (руб.)").replace(',', '.')),
                float(row.get("Стоимость продажи (руб.)").replace(',', '.')),
                int(row.get("Общий остаток")),
                float(row.get("Общая сумма остатков (руб.)").replace(',', '.')),
                float(row.get("Себест. (сумма) (руб.)").replace(',', '.')),
                float(row.get("Стоимость продажи (сумма) (руб.)").replace(',', '.')),
                int(row.get("Остаток на СДХ")),
                int(row.get("Остаток на фотостудии")),
                float(row.get("Остаток на СДХ (сумма) (руб.)").replace(',', '.')),
                int(row.get("Доступно к отправке")),
                id,
            ])
            count += 1
        logging.info("Count: %s", count)

def load_data(owner: str, fileName: str, con_id: str) -> None:
    r"""
    Загрузка данных в БД
    """
    PostgresHook(
        postgres_conn_id=con_id
    ).run(f"delete from dl.tmp_stock_ozon_stat where owner = '{owner}';")
    logging.info(f"Load data from file: {fileName}")
    PostgresHook(postgres_conn_id=con_id).copy_expert(
        "COPY dl.tmp_stock_kzexp(" +
        "owner, " +
        "date, " +
        "sku, " +
        "method, " +
        "category, " +
        "currency, " +
        "base_price, " +
        "discount_price, " +
        "premium_price, " +
        "ozon_card_price, " +
        "name, " +
        "brand, " +
        "seller, " +
        "warehouse, " +
        "warehouse_region, " +
        "warehouse_id, " +
        "quantity, " +
        "transaction_id, " +
        "barcode) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', HEADER FALSE, QUOTE E'\\b')",
        f'{fileName}')

