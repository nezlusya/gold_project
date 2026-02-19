import os

PG_CONN = {
    "host": os.getenv("PG_HOST"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
}

# TABLE_NAME = os.getenv("GOLD_SOURCE_TABLE")
# EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT", "gold_6m_forecast")
# FORECAST_HORIZON = int(os.getenv("FORECAST_HORIZON", 14))
