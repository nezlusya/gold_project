import pendulum
import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


OWNER = "Luda"
DAG_ID = "dm_gold_update_6"

SCHEMA = "dm"
TARGET_TABLE_1 = "gold_change_day"
TARGET_TABLE_2 = "gold_anomalies"
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
"""

SHORT_DESCRIPTION = ""

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 11, 18, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def build_dm(**context):
    con = duckdb.connect()
    con.sql("""
        CREATE SECRET pg_sec (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE 'postgres',
            USER 'gold_user',
            PASSWORD 'gold_pass'
        );
        """
        )
    con.sql("""
        ATTACH '' AS db (TYPE postgres, SECRET pg_sec);

        TRUNCATE db.dm.gold_change_day;
        INSERT INTO db.dm.gold_change_day
        SELECT
        date,
        buy_price,
        sell_price,
        ROUND(
            CASE
                WHEN LAG(buy_price) OVER (ORDER BY date) IS NULL OR LAG(buy_price) OVER (ORDER BY date) = 0
                    THEN NULL
                ELSE (buy_price - LAG(buy_price) OVER (ORDER BY date)) / LAG(buy_price) OVER (ORDER BY date) * 100
            END,
            2
        ) AS buy_change_pct,
        ROUND(
            CASE
                WHEN LAG(sell_price) OVER (ORDER BY date) IS NULL OR LAG(sell_price) OVER (ORDER BY date) = 0
                    THEN NULL
                ELSE (sell_price - LAG(sell_price) OVER (ORDER BY date)) / LAG(sell_price) OVER (ORDER BY date) * 100
            END,
            2
        ) AS sell_change_pct
        FROM db.ods.gold_price_cbr_6;

        -- Weekly
        TRUNCATE db.dm.gold_anomalies;
        INSERT INTO db.dm.gold_anomalies
        WITH stats AS (
            SELECT
                AVG(buy_price) AS avg_buy,
                STDDEV(buy_price) AS std_buy
            FROM db.ods.gold_price_cbr_6
        )
        SELECT
            s.date,
            s.buy_price,
            s.sell_price,
            -- доходность
            ROUND(
                CASE
                    WHEN LAG(s.buy_price) OVER (ORDER BY s.date) IS NULL OR
                         LAG(s.buy_price) OVER (ORDER BY s.date) = 0
                    THEN NULL
                    ELSE (s.buy_price - LAG(s.buy_price) OVER (ORDER BY s.date))
                         / LAG(s.buy_price) OVER (ORDER BY s.date)
                END, 6
            ) AS return,
            -- z-score
            ROUND(
                (s.buy_price - st.avg_buy) / st.std_buy,
                6
            ) AS zscore_buy,
            -- anomaly_flag
            CASE
                WHEN ABS((s.buy_price - st.avg_buy) / st.std_buy) > 3 THEN 1
                ELSE 0
            END AS anomaly_flag
        FROM db.ods.gold_price_cbr_6 s CROSS JOIN stats st
        ORDER BY s.date;
	""")
    con.close()


with DAG(
    dag_id = "dm_gold_update_6",
    schedule_interval="0 5 * * *",  # каждый день после ODS
    default_args=args,
    tags=["dm", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_runs=1,
    
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    wait_for_ods = ExternalTaskSensor(
        task_id="wait_for_ods",
        external_dag_id="from_minio_to_pg_6",
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=36000,
    )


    dm_builder = PythonOperator(
        task_id="build_dm",
        python_callable=build_dm
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_ods >> dm_builder >> end
