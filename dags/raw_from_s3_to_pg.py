import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


# --- Базовые параметры ---
OWNER = "Luda"
DAG_ID = "raw_from_s3_to_pg"

# Источники и цели
LAYER = "raw"
SOURCE = "gold_price_cbr"
SCHEMA = "ods"
TARGET_TABLE = "gold_price_cbr"

# Переменные из Airflow Variables
ACCESS_KEY = Variable.get("access_key", default_var="minioadmin")
SECRET_KEY = Variable.get("secret_key", default_var="minioadmin")
PASSWORD = Variable.get("pg_password", default_var="postgres")

# Метаданные
LONG_DESCRIPTION = """
# Загрузка данных о ценах на золото из MinIO в PostgreSQL
- Источник: s3://prod/raw/gold_price_cbr/YYYY-MM-DD/data.parquet
- Цель: таблица ods.gold_price_cbr
"""

SHORT_DESCRIPTION = "Загрузка котировок золота из MinIO (RAW) в PostgreSQL (ODS)"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(1998, 1, 5, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=30),
}


def get_dates(**context):
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


def transfer_to_pg(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start transfer for dates: {start_date}/{end_date}")

    con = duckdb.connect()

    # Настройка MinIO и PostgreSQL
    con.sql(f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'gold_user',
            PASSWORD '{PASSWORD}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
    """)


    # con.sql(f"""
    #     INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE} (date, buy_price, sell_price)
    #     ('2022-01-02', 3000, 5000);
    # """)


    try:
        # Загружаем данные из MinIO и вставляем в PostgreSQL
        con.sql(f"""
            INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE} (date, buy_price, sell_price)
            SELECT
                date,
                buy_price,
                sell_price
            FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/data.parquet';
        """)
    except duckdb.duckdb.HTTPException:
        print(f'file for {start_date} not found')
    finally:
        con.close()
    logging.info(f"✅ Transfer success for date: {start_date}")


# --- DAG ---
with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",  # после выгрузки newdag (через час)
    default_args=args,
    tags=["s3", "ods", "pg"],
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

    transfer_to_pg = PythonOperator(
        task_id="transfer_to_pg",
        python_callable=transfer_to_pg,
    )

    end = EmptyOperator(task_id="end")
    # start >> transfer_to_pg >> end
    start >> wait_for_raw_layer >> transfer_to_pg >> end
