import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor


OWNER = "Luda"
DAG_ID = "dm_gold_change_day"

SCHEMA = "dm"
TARGET_TABLE = "gold_change_day"
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# Динамика изменения цены золота по дням
- Источник: ods.gold_price_cbr
- Витрина: dm.gold_change_day
- Считаем разницу в % относительно предыдущего дня
"""

SHORT_DESCRIPTION = "Изменение цены золота по дням"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(1998, 1, 5, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",  # после ods и dm_gold_avg_price_month
    default_args=args,
    tags=["dm", "pg", "gold"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    wait_for_ods = ExternalTaskSensor(
        task_id="wait_for_ods",
        external_dag_id="raw_from_s3_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=36000,
    )

    drop_stg = SQLExecuteQueryOperator(
        task_id="drop_stg",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}";
        """,
    )

    create_stg = SQLExecuteQueryOperator(
        task_id="create_stg",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
        SELECT
            date,
            buy_price,
            sell_price,
            ROUND(
                (buy_price - LAG(buy_price) OVER (ORDER BY date)) / LAG(buy_price) OVER (ORDER BY date) * 100::FLOAT,
                2
            ) AS buy_change_pct,
            ROUND(
                (sell_price - LAG(sell_price) OVER (ORDER BY date)) / LAG(sell_price) OVER (ORDER BY date) * 100::FLOAT , 
                2
            ) AS sell_change_pct
        FROM ods.gold_price_cbr;
        """,
    )

    delete_old = SQLExecuteQueryOperator(
        task_id="delete_old",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE date IN (SELECT date FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}");
        """,
    )

    insert_new = SQLExecuteQueryOperator(
        task_id="insert_new",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}";
        """,
    )

    drop_stg_after = SQLExecuteQueryOperator(
        task_id="drop_stg_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}";
        """,
    )

    end = EmptyOperator(task_id="end")

    (
        start >>
        # wait_for_ods >>
        drop_stg >>
        create_stg >>
        delete_old >>
        insert_new >>
        drop_stg_after >>
        end
    )
