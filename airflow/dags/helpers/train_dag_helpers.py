"""
Training helpers: train Linear Regression, Ridge, and XGBoost against
transform_resale_flat_price; log every run to MLflow; save the winning
model (lowest test RMSE) as a pickle that the FastAPI /predict endpoint
can load directly.

Model pickle contract
---------------------
The saved pipeline accepts a 16-column DataFrame with the same column names
and dtypes as FEATURE_COLUMNS in api/app/model.py.  flat_model and town are
treated as numeric (label-encoded integers stored in transform_resale_flat_price).

MLflow
------
All runs are recorded under the experiment EXPERIMENT_NAME.  After training,
the winning run is tagged with best_model=true so it is easy to spot in the UI.
The MLflow tracking server is expected at MLFLOW_TRACKING_URI (default
http://localhost:9080 to match the existing notebooks).
"""

import logging
import os
import pathlib
import pickle
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBRegressor

try:
    from airflow.sdk.bases.hook import BaseHook          # Airflow 3.x
except ImportError:
    from airflow.hooks.base import BaseHook              # Airflow 2.x

from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — keep in sync with api/app/model.py FEATURE_COLUMNS
# ---------------------------------------------------------------------------

FEATURE_COLUMNS = [
    "flat_model", "floor_area_sqm", "max_floor_lvl", "total_dwelling_units",
    "storey_mid", "remaining_lease_years", "town",
    "dist_to_nearest_mrt_m", "n_mrt_within_1km",
    "dist_to_nearest_bus_stop_m", "n_bus_stop_within_1km",
    "month_index",
    "dist_to_food_m", "n_food_within_1km",
    "dist_to_supermarket_m", "n_supermarket_within_1km",
]
TARGET_COL = "resale_price"

# flat_model and town are stored as strings in transform_resale_flat_price
# (e.g. 'Apartment', 'Jurong West').  Everything else is numeric.
CATEGORICAL_COLS = ["flat_model", "town"]
NUMERICAL_COLS = [c for c in FEATURE_COLUMNS if c not in CATEGORICAL_COLS]

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:9080")
EXPERIMENT_NAME = "HDB Resale Price Prediction: Auto Training"

# Path where the winning pickle is written for the API to consume.
# Override via TRAINED_MODEL_OUTPUT_PATH env var when running in Docker / a
# non-standard layout.
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_MODEL_OUTPUT_PATH = str(_PROJECT_ROOT / "api" / "models" / "model.pkl")

# URL of the FastAPI /reload-model endpoint.  Set API_RELOAD_URL to match your
# deployment (e.g. http://api:7860/reload-model inside Docker Compose).
DEFAULT_API_RELOAD_URL = "http://localhost:7860/reload-model"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_engine(mysql_conn_id: str):
    conn = BaseHook.get_connection(mysql_conn_id)
    return create_engine(
        f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )


def _load_data(engine) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """Load features + target from transform table and return train/test splits."""
    col_list = ", ".join(f"`{c}`" for c in FEATURE_COLUMNS + [TARGET_COL])
    df = pd.read_sql(
        f"SELECT {col_list} FROM transform_resale_flat_price",
        con=engine,
    )
    logger.info("Loaded %d rows from transform_resale_flat_price", len(df))

    before = len(df)
    df = df.dropna()
    dropped = before - len(df)
    if dropped:
        null_rate = dropped / before
        logger.warning("Dropped %d rows with nulls (%.1f%%)", dropped, null_rate * 100)
        if null_rate > 0.10:
            raise ValueError(
                f"Null drop removed {null_rate:.1%} of rows — exceeds 10% threshold. "
                "Investigate transform_resale_flat_price for data quality issues."
            )

    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COL]
    return train_test_split(X, y, test_size=0.2, random_state=42, shuffle=True)


def _regression_metrics(y_true: pd.Series, y_pred: np.ndarray) -> dict:
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    mape = float(np.mean(np.abs((y_true - y_pred) / y_true)) * 100)
    r2 = float(r2_score(y_true, y_pred))
    return dict(test_rmse=rmse, test_mae=mae, test_mape=mape, test_r2=r2)


def _make_pipeline(estimator) -> Pipeline:
    """
    Build a full preprocessing + model pipeline.

    Numerical columns: median imputation → StandardScaler
    Categorical columns (flat_model, town — stored as strings):
        most-frequent imputation → OneHotEncoder
    """
    num_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])
    cat_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    preprocessor = ColumnTransformer([
        ("num", num_pipe, NUMERICAL_COLS),
        ("cat", cat_pipe, CATEGORICAL_COLS),
    ])
    return Pipeline([
        ("preprocessor", preprocessor),
        ("model", estimator),
    ])


# ---------------------------------------------------------------------------
# Candidate models
# ---------------------------------------------------------------------------

def _candidates() -> list[tuple[str, dict, object]]:
    """Return (name, mlflow_params, estimator) triples."""
    return [
        (
            "LinearRegression",
            {"model_type": "LinearRegression"},
            LinearRegression(),
        ),
        (
            "Ridge",
            {"model_type": "Ridge", "alpha": 1.0},
            Ridge(alpha=1.0),
        ),
        (
            "XGBoost",
            {
                "model_type": "XGBoost",
                "n_estimators": 300,
                "learning_rate": 0.05,
                "max_depth": 6,
            },
            XGBRegressor(
                n_estimators=300,
                learning_rate=0.05,
                max_depth=6,
                random_state=42,
                verbosity=0,
            ),
        ),
    ]


# ---------------------------------------------------------------------------
# Public task functions
# ---------------------------------------------------------------------------

def train_and_select_best(mysql_conn_id: str) -> None:
    """
    Train all candidate models, log each run to MLflow, then save the
    model with the lowest test RMSE as a pickle at TRAINED_MODEL_OUTPUT_PATH.
    """
    engine = _get_engine(mysql_conn_id)
    X_train, X_test, y_train, y_test = _load_data(engine)
    logger.info("Dataset split — train: %d rows, test: %d rows", len(X_train), len(X_test))

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    run_month = datetime.utcnow().strftime("%Y-%m")

    best_rmse = float("inf")
    best_pipeline: Pipeline | None = None
    best_run_id: str | None = None
    best_name: str | None = None

    for name, params, estimator in _candidates():
        pipeline = _make_pipeline(estimator)
        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_test)
        metrics = _regression_metrics(y_test, y_pred)

        logger.info(
            "%s — RMSE=%.0f  MAE=%.0f  MAPE=%.2f%%  R²=%.4f",
            name,
            metrics["test_rmse"],
            metrics["test_mae"],
            metrics["test_mape"],
            metrics["test_r2"],
        )

        with mlflow.start_run(run_name=f"{name}_{run_month}") as run:
            mlflow.log_params({**params, "training_month": run_month})
            mlflow.log_metrics(metrics)
            mlflow.set_tag("pipeline_stage", "auto_train")

            signature = infer_signature(X_train, pipeline.predict(X_train))
            mlflow.sklearn.log_model(
                sk_model=pipeline,
                artifact_path="model",
                signature=signature,
                input_example=X_train.head(5),
            )

        if metrics["test_rmse"] < best_rmse:
            best_rmse = metrics["test_rmse"]
            best_pipeline = pipeline
            best_run_id = run.info.run_id
            best_name = name

    logger.info("Winner: %s (test RMSE=%.0f)", best_name, best_rmse)

    # Tag the winning run so it is clearly visible in the MLflow UI.
    with mlflow.start_run(run_id=best_run_id):
        mlflow.set_tag("best_model", "true")
        mlflow.set_tag("best_model_name", best_name)

    # Write the winning pipeline as a pickle for the API.
    output_path = pathlib.Path(
        os.getenv("TRAINED_MODEL_OUTPUT_PATH", DEFAULT_MODEL_OUTPUT_PATH)
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump(best_pipeline, f)
    logger.info("Saved %s pipeline to %s", best_name, output_path)


def reload_api_model(mysql_conn_id: str = None) -> None:
    """
    POST to the FastAPI /reload-model endpoint so the running server
    hot-swaps to the newly written pickle without a restart.

    This is best-effort — if the API is unreachable (e.g. it has not been
    started yet) the failure is logged as a warning rather than crashing
    the DAG.  The API will pick up the new model on its next cold start.
    """
    import requests

    url = os.getenv("API_RELOAD_URL", DEFAULT_API_RELOAD_URL)
    try:
        resp = requests.post(url, timeout=30)
        resp.raise_for_status()
        logger.info("API model reloaded: %s", resp.json())
    except Exception as exc:
        logger.warning(
            "Could not reach API reload endpoint at %s (%s). "
            "The API will load the new model on next restart.",
            url,
            exc,
        )
