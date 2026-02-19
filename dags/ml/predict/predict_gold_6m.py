import os
import pandas as pd
import numpy as np
import psycopg2
import mlflow.pyfunc

from datetime import timedelta
from ml.config import PG_CONN


MODEL_NAME = "gold_price_forecast"
FORECAST_HORIZON = 14

SCHEMA = "cdm"
TABLE = "gold_forecast"


# -----------------------------
# load model
# -----------------------------

def load_model():

    model = mlflow.pyfunc.load_model(
        f"models:/{MODEL_NAME}/Production"
    )

    return model


# -----------------------------
# load latest data
# -----------------------------

def load_data():

    conn = psycopg2.connect(**PG_CONN)

    df = pd.read_sql(
        """
        SELECT date, buy_price
        FROM ods.gold_price_cbr_6
        ORDER BY date
        """,
        conn
    )

    conn.close()

    df["date"] = pd.to_datetime(df["date"])

    return df


# -----------------------------
# predict
# -----------------------------

def make_future_df(last_date):

    future_dates = pd.date_range(
        start=last_date + timedelta(days=1),
        periods=FORECAST_HORIZON
    )

    return pd.DataFrame({
        "date": future_dates
    })


def save_forecast(df):

    conn = psycopg2.connect(**PG_CONN)
    cursor = conn.cursor()

    cursor.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA}
    """)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
            date DATE,
            forecast FLOAT
        )
    """)

    cursor.execute(f"TRUNCATE TABLE {SCHEMA}.{TABLE}")

    for _, row in df.iterrows():

        cursor.execute(
            f"""
            INSERT INTO {SCHEMA}.{TABLE}
            VALUES (%s, %s)
            """,
            (row["date"], float(row["forecast"]))
        )

    conn.commit()
    conn.close()


# -----------------------------
# main
# -----------------------------

def main():

    print("Loading model")

    model = load_model()

    df = load_data()

    last_date = df["date"].max()

    future_df = make_future_df(last_date)

    preds = model.predict(future_df)

    future_df["forecast"] = preds

    save_forecast(future_df)
    max_future_date = future_df['date'].max()
    print("Forecast saved")
    print(f'dates {last_date} - {max_future_date}') 

if __name__ == "__main__":

    main()
