import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

import mlflow
from mlflow.tracking import MlflowClient
import logging

from ml.promote.promote_gold_6m import main

OWNER = "Luda"
DAG_ID = "ml_train_gold_6m"

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
    "retries": 1,
}

with DAG(
    dag_id="ml_promote_gold_6m",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["ml", "promote"],
    description="Promoting the best model in production",
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    promote = PythonOperator(
        task_id="promote_model",
        python_callable=main
    )

    # trigger_predict = TriggerDagRunOperator(
    #     task_id="trigger_predict",
    #     trigger_dag_id="ml_predict_gold_6m",
    # )

    end = EmptyOperator(
        task_id="end"
    )

    start >> promote >> end
