import logging
import os
import pathlib
import pickle

import numpy as np
from fastapi import HTTPException

logger = logging.getLogger(__name__)

# Ordered list of feature names — must match the order the model was trained on
FEATURE_COLUMNS = [
    "flat_type", "floor_area_sqm", "flat_model", "storey_mid", "month", "year",
    "quarter", "month_index", "max_floor_lvl", "total_dwelling_units",
    "has_market_hawker", "has_multistorey_carpark", "year_completed", "building_age",
    "remaining_lease_years", "lease_age", "lease_age_sq", "latitude", "longitude",
    "planning_area", "region", "town", "dist_to_nearest_mrt_m", "n_mrt_within_1km",
    "dist_to_school_m", "n_school_within_1km", "dist_to_mall_m", "n_mall_within_1km",
    "dist_to_food_m", "n_food_within_1km", "dist_to_park_m", "n_park_within_1km",
    "dist_to_supermarket_m", "n_supermarket_within_1km", "dist_to_nearest_carpark_m",
    "n_carparks_within_500m", "gantry_height", "car_park_decks", "has_free_parking",
    "has_short_term_parking", "has_night_parking", "has_car_park_basement",
    "dist_to_nearest_bus_stop_m", "n_bus_stop_within_1km",
    "nearest_bus_stop_operating_days_per_week", "nearest_bus_stop_busyness_level",
    "dist_to_nearest_tourist_attraction_m", "transport_school_pct_bus",
    "transport_school_pct_mrt", "transport_school_pct_mrt_bus",
    "transport_school_pct_car", "tenancy_pct_owner", "dwelling_pct_1room",
    "dwelling_pct_2room", "dwelling_pct_3room", "dwelling_pct_4room",
    "dwelling_pct_5room", "dwelling_pct_exec", "dwelling_pct_studio",
    "dwelling_pct_multi_gen", "floor_area_x_storey", "storey_ratio",
    "town_price_trend_6m",
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
        X = np.array([[features[col] for col in FEATURE_COLUMNS]])
    except KeyError as e:
        raise HTTPException(status_code=422, detail=f"Missing feature: {e}")

    try:
        result = _model.predict(X)
        return float(np.ravel(result)[0])
    except Exception as e:
        logger.error("Model inference failed: %s", e)
        raise HTTPException(status_code=500, detail="Model inference failed")
