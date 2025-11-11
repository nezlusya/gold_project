import logging
import pandas as pd
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.sensors.external_task import ExternalTaskSensor

import clickhouse_connect

# --- Настройки DAG ---
OWNER = "Luda"
DAG_ID = "raw_from_s3_to_clickhouse"

LAYER = "raw"
SOURCE = "gold_price_cbr"
CLICKHOUSE_DB = "default"
CLICKHOUSE_TABLE = "gold_price_cbr"

# Переменные Airflow
ACCESS_KEY = Variable.get("access_key", default_var="minioadmin")
SECRET_KEY = Variable.get("secret_key", default_var="minioadmin")
CLICKHOUSE_HOST = Variable.get("clickhouse_host", default_var="clickhouse")
CLICKHOUSE_USER = Variable.get("clickhouse_user", default_var="default")
CLICKHOUSE_PASSWORD = Variable.get("clickhouse_password", default_var="")

LONG_DESCRIPTION = """
# Загрузка цен на золото из MinIO в ClickHouse
- Источник: s3://prod/raw/gold_price_cbr/YYYY-MM-DD/data.parquet
- Цель: ClickHouse таблица default.gold_price_cbr
"""

SHORT_DESCRIPTION = "Загрузка котировок золота из MinIO (RAW) в ClickHouse"

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(1998, 1, 5, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=30),
}

# --- Получение дат интервала ---
def get_dates(**context):
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date

# --- Функция загрузки в ClickHouse ---
def transfer_to_clickhouse(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start transfer for dates: {start_date}/{end_date}")

    # Чтение данных из MinIO через DuckDB
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

    try:
        df = con.execute(f"""
            SELECT * FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/data.parquet'
        """).df()
    except duckdb.duckdb.HTTPException:
        logging.warning(f"⚠️ No data found for {start_date}")
        con.close()
        return
    finally:
        con.close()


    # Подключение к ClickHouse через get_client
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

    # Создание таблицы при необходимости
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
            date Date,
            buy_price Float64,
            sell_price Float64
        ) ENGINE = MergeTree()
        ORDER BY date
        SETTINGS index_granularity = 1024;
    """)

    # Вставка данных
    client.insert_df(CLICKHOUSE_TABLE, df)
    logging.info(f"✅ Transfer success for date: {start_date}")

# --- DAG ---
with DAG(
    dag_id=DAG_ID,
    schedule="0 5 * * *",  # после выгрузки в MinIO
    default_args=default_args,
    tags=["s3", "clickhouse", "gold"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    wait_for_raw_layer = ExternalTaskSensor(
        task_id="wait_for_raw_layer",
        external_dag_id="newdag",  # ждем DAG, который грузит в MinIO
        allowed_states=["success"],
        mode="reschedule",
        timeout=36000,
        poke_interval=60,
    )

    transfer_to_clickhouse = PythonOperator(
        task_id="transfer_to_clickhouse",
        python_callable=transfer_to_clickhouse,
    )

    end = EmptyOperator(task_id="end")
    # start >> transfer >> end
    start >> wait_for_raw_layer >> transfer_to_clickhouse >> end
