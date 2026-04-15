import logging
import os
import pathlib
import pickle

import numpy as np
import pandas as pd
from fastapi import HTTPException

logger = logging.getLogger(__name__)

# Ordered list of feature names — must match the order the model was trained on
FEATURE_COLUMNS = [
    "flat_model", "floor_area_sqm", "max_floor_lvl", "total_dwelling_units",
    "storey_mid", "remaining_lease_years", "town", "dist_to_nearest_mrt_m",
    "n_mrt_within_1km", "dist_to_nearest_bus_stop_m", "n_bus_stop_within_1km",
    "month_index", "dist_to_food_m", "n_food_within_1km",
    "dist_to_supermarket_m", "n_supermarket_within_1km",
]

DUMMY_PREDICTION = 500_000.0

_model = None
_is_dummy = True


def load_model() -> None:
    global _model, _is_dummy
    default_path = pathlib.Path(__file__).parent.parent / "models" / "model.pkl"
    model_path = os.getenv("MODEL_PATH", str(default_path))
    if os.path.exists(model_path):
        try:
            with open(model_path, "rb") as f:
                _model = pickle.load(f)
            _is_dummy = False
            logger.info("Loaded model from %s", model_path)
        except Exception as e:
            logger.error("Failed to load model from %s: %s — falling back to DUMMY mode", model_path, e)
            _model = None
            _is_dummy = True
    else:
        _model = None
        _is_dummy = True
        logger.warning("No model found at %s — running in DUMMY mode (returns %.1f)", model_path, DUMMY_PREDICTION)


def predict(features: dict) -> float:
    if _is_dummy:
        return DUMMY_PREDICTION

    try:
        X = pd.DataFrame([{col: features[col] for col in FEATURE_COLUMNS}])
    except KeyError as e:
        raise HTTPException(status_code=422, detail=f"Missing feature: {e}")

    try:
        log_pred = _model.predict(X)
        return float(np.expm1(np.ravel(log_pred)[0]))
    except Exception as e:
        logger.error("Model inference failed: %s", e)
        raise HTTPException(status_code=500, detail="Model inference failed")
