import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging

import mlflow

# импорт train функции
from ml.train.train_gold_6m import main as train_gold_6m


OWNER = "Luda"
DAG_ID = "ml_train_gold_6m"

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
    "retries": 1,
}


# ============================================
# TASK WRAPPER (ВАЖНО)
# ============================================

def run_training():

    logging.info("Starting gold price training pipeline")

    # проверка подключения к MLflow
    logging.info("MLflow URI:", mlflow.get_tracking_uri())

    # запуск обучения
    train_gold_6m()

    logging.info("Training finished successfully")


# ============================================
# DAG
# ============================================

with DAG(
    dag_id=DAG_ID,
    schedule="0 9 * * 0",  # воскресенье 19:00
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ml", "training"],
    description="Train gold forecast model and register best model in MLflow Registry",
) as dag:

    start = EmptyOperator(
        task_id="start"
    )


    train = PythonOperator(
        task_id="train_gold_models",
        python_callable=run_training,
        execution_timeout=pendulum.duration(minutes=60),
    )


    trigger_promote = TriggerDagRunOperator(
        task_id="trigger_promote",
        trigger_dag_id="ml_promote_gold_6m",
    )


    end = EmptyOperator(
        task_id="end"
    )


    start >> train >> trigger_promote >> end
