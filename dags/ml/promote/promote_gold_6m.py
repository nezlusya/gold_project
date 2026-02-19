import mlflow
import logging
from mlflow.tracking import MlflowClient

EXPERIMENT_NAME = "gold_6m_forecast"
MODEL_NAME = "gold_price_forecast"
MLFLOW_URI = "http://mlflow:5000"


QUALITY_THRESHOLD = 600

def main():
    mlflow.set_tracking_uri(MLFLOW_URI)
    client = MlflowClient()

    # 1️ Получаем эксперимент
    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment is None:
        raise RuntimeError("Experiment not found")

    # 2️ Берём лучший run по RMSE
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["metrics.rmse ASC"],
        max_results=1
    )

    if not runs:
        raise RuntimeError("No runs found")

    best_run = runs[0]
    best_rmse = best_run.data.metrics["rmse"]
    best_run_id = best_run.info.run_id

    logging.info(f"Best candidate RMSE = {best_rmse}")

    # 3️ QUALITY GATE
    if best_rmse > QUALITY_THRESHOLD:
        logging.info("❌ Quality gate failed. Promotion aborted.")
        return

    # 4️ Регистрируем модель (или новую версию)
    model_uri = f"runs:/{best_run_id}/model"
    registered_model = mlflow.register_model(
        model_uri=model_uri,
        name=MODEL_NAME
    )

    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=registered_model.version,
        stage="Staging",
        archive_existing_versions=False
    )

    # 5️ Проверяем Production (ROLLBACK LOGIC)
    prod_versions = client.get_latest_versions(
        MODEL_NAME, stages=["Production"]
    )

    if prod_versions:
        prod_version = prod_versions[0]
        prod_run = client.get_run(prod_version.run_id)
        prod_rmse = prod_run.data.metrics["rmse"]

        logging.info(f"Production RMSE = {prod_rmse}")

        # 🔁 rollback при деградации
        if best_rmse >= prod_rmse:
            logging.info("❌ New model worse than Production — rollback")
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=registered_model.version,
                stage="Archived"
            )
            return

    # 6️ Promote to Production
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=registered_model.version,
        stage="Production",
        archive_existing_versions=True
    )

    logging.info("✅ Model promoted to Production")
