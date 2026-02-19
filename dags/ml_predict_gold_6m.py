import pendulum

from airflow import DAG
from airflow.models import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import logging

from ml.predict.predict_gold_6m import main


OWNER = "Luda"
DAG_ID = "ml_predict_gold_6m"

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
    "retries": 1,
}

# ============================================
def check_dag_success(**context):
    dr = DagRun.find(dag_id='ml_train_gold_6m')
    latest_run = dr[0] if dr else None
    
    if latest_run and latest_run.state == 'success':
        logging.info("Последний запуск ml_train_gold_6m успешен")
        return True
    else:
        raise Exception("Последний запуск ml_train_gold_6m НЕ успешен")

# ============================================
# DAG
# ============================================

with DAG(
    dag_id=DAG_ID,
    schedule="0 10 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ml", "prediction"],
    description="Daily predictions using the best model",
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    check_task = PythonOperator(
        task_id='check_previous_dag',
        python_callable=check_dag_success
    )

    predict = PythonOperator(
        task_id="predict",
        python_callable=main
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> check_task >> predict >> end
