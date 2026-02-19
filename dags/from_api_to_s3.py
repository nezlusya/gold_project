import logging
import pendulum
import pandas as pd
import io
import requests
import xml.etree.ElementTree as ET
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import duckdb


OWNER = "Luda"
DAG_ID = "from_api_to_s3"

LAYER = "raw"
SOURCE = "gold_price_cbr"

ACCESS_KEY = Variable.get("access_key", default_var="DUMMY_KEY")
SECRET_KEY = Variable.get("secret_key", default_var="DUMMY_SECRET")

LONG_DESCRIPTION = """
# Загрузка цен на золото с сайта ЦБ РФ
- Динамическая дата: вчера -> сегодня
- Сохраняем в MinIO в parquet по пути: s3://prod/raw/gold_price_cbr/YYYY-MM-DD/
"""

SHORT_DESCRIPTION = "Загрузка цен на золото с ЦБ РФ в MinIO"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(1998, 1, 5, tz="Europe/Moscow"),
    "catchup": True,  # Изменено на True для обработки исторических данных
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=30),
}


def extract_and_upload_to_minio(**context):
    """Извлекает данные о ценах на золото и загружает в MinIO"""
    # Получаем дату выполнения DAG
    execution_date = context["data_interval_start"]
    date_str = execution_date.format("DD/MM/YYYY")
    date_path = execution_date.format("YYYY-MM-DD")

    logging.info(f"💻 Start load for date: {date_str}")

    # Запрос данных за конкретный день
    url = f"http://www.cbr.ru/scripts/XML_metall.asp?date_req1={date_str}&date_req2={date_str}"
    response = requests.get(url)
    if response.status_code != 200:
        logging.warning(f"⚠️ No data for date: {date_str}, status: {response.status_code}")
        return  # Пропускаем дни без данных

    root = ET.fromstring(response.content)
    gold_prices = []

    for record in root.findall("Record"):
        if record.get("Code") == "1":  # Код для золота
            date = pd.to_datetime(record.get("Date"), format="%d.%m.%Y")
            buy = float(record.find("Buy").text.replace(",", "."))
            sell = float(record.find("Sell").text.replace(",", "."))
            gold_prices.append({"date": date, "buy_price": buy, "sell_price": sell})

    if not gold_prices:
        logging.warning(f"⚠️ No gold price data for date: {date_str}")
        return

    df = pd.DataFrame(gold_prices)

    # Подключаемся к DuckDB и настраиваем MinIO
    con = duckdb.connect()

    con.sql(f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
    """)

    # Регистрируем DataFrame как временную таблицу
    con.register('gold_prices_df', df)

    # Сохраняем в MinIO в формате Parquet
    s3_path = f"s3://prod/{LAYER}/{SOURCE}/{date_path}/data.parquet"

    con.sql(f"""
        COPY gold_prices_df 
        TO '{s3_path}' 
        (FORMAT PARQUET, CODEC 'GZIP');
    """)

    con.close()
    logging.info(f"✅ Upload success for date: {date_str}")


with DAG(
        dag_id=DAG_ID,
        schedule_interval="0 5 * * *",  # Ежедневно в 5 утра
        default_args=args,
        tags=["s3", "raw"],
        description=SHORT_DESCRIPTION,
        max_active_tasks=1,
        max_active_runs=1,
        catchup=True,  # Включить обработку исторических данных
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")
    extract_and_upload = PythonOperator(
        task_id="extract_and_upload_to_minio",
        python_callable=extract_and_upload_to_minio,
        provide_context=True,
    )
    end = EmptyOperator(task_id="end")

    start >> extract_and_upload >> end