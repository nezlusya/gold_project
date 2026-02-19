import logging
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import clickhouse_connect

OWNER = "Luda"
DAG_ID = "dm_gold_price_monthly"

CLICKHOUSE_DB = "default"
CLICKHOUSE_TABLE_SRC = "gold_price_cbr"
CLICKHOUSE_TABLE_DM = "dm_gold_price_monthly"

CLICKHOUSE_HOST = Variable.get("clickhouse_host", default_var="clickhouse")
CLICKHOUSE_USER = Variable.get("clickhouse_user", default_var="default")
CLICKHOUSE_PASSWORD = Variable.get("clickhouse_password", default_var="")

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(1998, 2, 1, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=30),
    "catchup": True,
}

def build_dm(**kwargs):
    execution_date = kwargs['execution_date']
    month_start = execution_date.start_of("month").to_date_string()
    logging.info(f"🏗️ Building dm_gold_price_monthly for {month_start}...")

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )

    try:
        # Создаем таблицу, если нет
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE_DM} (
                month Date,
                avg_buy_price Float64,
                avg_sell_price Float64,
                min_buy_price Float64,
                max_buy_price Float64,
                volatility Float64,
                median_buy_price Float64,
                q1_buy_price Float64,
                q3_buy_price Float64,
                iqr_buy_price Float64,
                month_return Float64,
                trend_direction String,
                above_avg_days UInt32,
                max_drawdown Float64
            ) ENGINE = MergeTree()
            ORDER BY month
        """)

        # Удаляем данные за текущий месяц
        client.command(f"""
            ALTER TABLE {CLICKHOUSE_TABLE_DM} DELETE WHERE month = toStartOfMonth(toDate('{month_start}'))
        """)

        # Вставляем агрегированные данные за месяц
        client.command(f"""
            INSERT INTO {CLICKHOUSE_TABLE_DM}
            WITH prev_month_avg AS (
                SELECT avg(buy_price) AS prev_avg
                FROM {CLICKHOUSE_TABLE_SRC}
                WHERE date >= addMonths(toStartOfMonth(toDate('{month_start}')), -1)
                  AND date < toStartOfMonth(toDate('{month_start}'))
            ),
            current_month_stats AS (
                SELECT
                    toStartOfMonth(date) AS month,
                    avg(buy_price) AS avg_buy_price,
                    avg(sell_price) AS avg_sell_price,
                    min(buy_price) AS min_buy_price,
                    max(buy_price) AS max_buy_price,
                    stddevPop(buy_price) AS volatility,
                    median(buy_price) AS median_buy_price,
                    quantile(0.25)(buy_price) AS q1_buy_price,
                    quantile(0.75)(buy_price) AS q3_buy_price,
                    (quantile(0.75)(buy_price) - quantile(0.25)(buy_price)) AS iqr_buy_price,
                    max(buy_price) - min(buy_price) AS max_drawdown
                FROM {CLICKHOUSE_TABLE_SRC}
                WHERE date >= toStartOfMonth(toDate('{month_start}'))
                  AND date < addMonths(toStartOfMonth(toDate('{month_start}')), 1)
                GROUP BY month
            ),
            above_avg_counts AS (
                SELECT
                    toStartOfMonth(date) AS month,
                    countIf(buy_price > (SELECT avg_buy_price FROM current_month_stats)) AS above_avg_days
                FROM {CLICKHOUSE_TABLE_SRC}
                WHERE date >= toStartOfMonth(toDate('{month_start}'))
                  AND date < addMonths(toStartOfMonth(toDate('{month_start}')), 1)
                GROUP BY month
            )
            SELECT
                cs.month,
                cs.avg_buy_price,
                cs.avg_sell_price,
                cs.min_buy_price,
                cs.max_buy_price,
                cs.volatility,
                cs.median_buy_price,
                cs.q1_buy_price,
                cs.q3_buy_price,
                cs.iqr_buy_price,
                (cs.avg_buy_price - pma.prev_avg) / NULLIF(pma.prev_avg, 0) AS month_return,
                IF(
                    cs.avg_buy_price > pma.prev_avg, 'up',
                    IF(cs.avg_buy_price < pma.prev_avg, 'down', 'flat')
                ) AS trend_direction,
                aac.above_avg_days,
                cs.max_drawdown
            FROM current_month_stats cs
            CROSS JOIN prev_month_avg pma
            JOIN above_avg_counts aac ON cs.month = aac.month
        """)

        logging.info(f"✅ Successfully rebuilt monthly DM for {month_start}")

    except Exception as e:
        logging.error(f"Error building dm_gold_price_monthly for {month_start}: {e}")
        raise
    finally:
        client.close()

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 3 1 * *",
    default_args=default_args,
    tags=["dm", "clickhouse"],
    max_active_runs=1,
    catchup=True,
    description="Monthly gold price DM with extended analytics",
) as dag:

    start = EmptyOperator(task_id="start")

    build_dm_task = PythonOperator(
        task_id="build_dm",
        python_callable=build_dm,
    )

    end = EmptyOperator(task_id="end")

    start >> build_dm_task >> end
