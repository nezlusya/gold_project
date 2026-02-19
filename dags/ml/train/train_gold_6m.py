import os
import pandas as pd
import numpy as np
import psycopg2
import mlflow
import mlflow.pyfunc

from datetime import timedelta

from sklearn.metrics import mean_squared_error, mean_absolute_error
from xgboost import XGBRegressor
from statsmodels.tsa.statespace.sarimax import SARIMAX

from ml.config import PG_CONN


# -----------------------------
# CONFIG
# -----------------------------

SCHEMA = "ods"
TABLE_NAME = "gold_price_cbr_6"

EXPERIMENT_NAME = "gold_6m_forecast"
MODEL_NAME = "gold_6m_forecast"

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

FORECAST_HORIZON = 14
QUALITY_THRESHOLD = 500


# -----------------------------
# MLflow init
# -----------------------------

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment(EXPERIMENT_NAME)


# -----------------------------
# Load data
# -----------------------------

def load_data():

    conn = psycopg2.connect(**PG_CONN)

    df = pd.read_sql(
        f"""
        SELECT date, buy_price
        FROM {SCHEMA}.{TABLE_NAME}
        ORDER BY date
        """,
        conn,
    )

    conn.close()

    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    return df


# -----------------------------
# Split data
# -----------------------------

def split_data(df):

    train = df[:-FORECAST_HORIZON]
    test = df[-FORECAST_HORIZON:]

    return train, test


# -----------------------------
# Metrics
# -----------------------------

def calc_metrics(y_true, y_pred):

    return {

        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred))
    }


# -----------------------------
# Moving Average model
# -----------------------------

class MovingAverageModel(mlflow.pyfunc.PythonModel):

    def __init__(self, window=7):
        self.window = window
        self.last_values = None

    def fit(self, series):
        self.last_values = series[-self.window:]

    def predict(self, context, model_input):
        return np.repeat(np.mean(self.last_values), len(model_input))


def train_moving_average(train, test):

    model = MovingAverageModel(window=7)
    model.fit(train["buy_price"])

    preds = np.repeat(np.mean(train["buy_price"][-7:]), len(test))

    return model, preds


# -----------------------------
# SARIMAX
# -----------------------------

class SarimaxWrapper(mlflow.pyfunc.PythonModel):

    def __init__(self, model):
        self.model = model

    def predict(self, context, model_input):
        steps = len(model_input)
        forecast = self.model.forecast(steps=steps)
        return forecast.values


def train_sarimax(train, test):

    model = SARIMAX(
        train["buy_price"],
        order=(1,1,1),
        seasonal_order=(1,1,1,7)
    ).fit(disp=False)

    preds = model.forecast(steps=len(test))

    return model, preds


# -----------------------------
# XGBoost
# -----------------------------

class XGBWrapper(mlflow.pyfunc.PythonModel):

    def __init__(self, model):
        self.model = model

    def predict(self, context, model_input):
        return self.model.predict(model_input)


def create_features(df):

    df = df.copy()

    df["lag1"] = df["buy_price"].shift(1)
    df["lag7"] = df["buy_price"].shift(7)

    df["rolling_mean7"] = df["buy_price"].rolling(7).mean()

    df = df.dropna()

    return df


def train_xgboost(train, test):

    df = pd.concat([train, test])
    df_feat = create_features(df)

    train_feat = df_feat.loc[train.index.intersection(df_feat.index)]
    test_feat = df_feat.loc[test.index.intersection(df_feat.index)]

    X_train = train_feat.drop(columns=["buy_price"])
    y_train = train_feat["buy_price"]

    X_test = test_feat.drop(columns=["buy_price"])

    model = XGBRegressor(
        n_estimators=200,
        max_depth=3,
        learning_rate=0.05
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    return model, preds


# -----------------------------
# Main pipeline
# -----------------------------

def main():

    print("Loading data...")

    df = load_data()

    train, test = split_data(df)

    best_rmse = float("inf")
    best_run_id = None

    models = {

        "MovingAverage": train_moving_average,
        "SARIMAX": train_sarimax,
        "XGBoost": train_xgboost
    }


    for model_name, trainer in models.items():

        print(f"Training {model_name}")

        with mlflow.start_run(run_name=model_name) as run:

            model, preds = trainer(train, test)

            metrics = calc_metrics(
                test["buy_price"][:len(preds)],
                preds
            )

            mlflow.log_metrics(metrics)

            print(model_name, metrics)

            if model_name == "MovingAverage":

                mlflow.pyfunc.log_model(
                    artifact_path="model",
                    python_model=model
                )

            elif model_name == "SARIMAX":

                mlflow.pyfunc.log_model(
                    artifact_path="model",
                    python_model=SarimaxWrapper(model)
                )

            elif model_name == "XGBoost":

                mlflow.pyfunc.log_model(
                    artifact_path="model",
                    python_model=XGBWrapper(model)
                )

            if metrics["rmse"] < best_rmse:

                best_rmse = metrics["rmse"]
                best_run_id = run.info.run_id


    print("Best RMSE:", best_rmse)


    # Quality gate
    if best_rmse > QUALITY_THRESHOLD:

        print("Quality gate failed")
        return


    # Register model
    model_uri = f"runs:/{best_run_id}/model"

    client = mlflow.tracking.MlflowClient()

    result = mlflow.register_model(
        model_uri,
        MODEL_NAME
    )

    print("Registered version:", result.version)


    # Promote to Production
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=result.version,
        stage="Production"
    )

    print("Promoted to Production")


if __name__ == "__main__":

    main()
