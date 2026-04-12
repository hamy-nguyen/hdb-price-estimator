import logging

import pandas as pd
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)

FEATURES_SELECTED = [
    "flat_model", "floor_area_sqm", "max_floor_lvl", "total_dwelling_units",
    "storey_mid", "remaining_lease_years", "town",
    "dist_to_nearest_mrt_m", "n_mrt_within_1km", "dist_to_nearest_bus_stop_m",
    "n_bus_stop_within_1km", "month_index", "dist_to_food_m", "n_food_within_1km",
    "dist_to_supermarket_m", "n_supermarket_within_1km",
]
TARGET_COL = "resale_price"
CATEGORICAL_COLS = ["flat_model", "town"]


def load_ml_datasets(engine, test_size=0.2, random_state=42):
    """
    Extract features from transform_resale_flat_price, drop nulls,
    one-hot encode categoricals, and return train/test splits.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        e.g. create_engine("mysql+pymysql://user:pass@host:3306/dbname")

    Returns
    -------
    X_train, X_test : pd.DataFrame  — feature matrices
    y_train, y_test : pd.Series     — resale_price target vectors
    """
    col_list = ", ".join(f"`{c}`" for c in FEATURES_SELECTED + [TARGET_COL])
    df = pd.read_sql(f"SELECT {col_list} FROM transform_resale_flat_price", con=engine)
    logger.info("Loaded %d rows", len(df))

    # Null audit and drop
    null_counts = df.isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]
    if not cols_with_nulls.empty:
        logger.warning("Nulls detected:\n%s", cols_with_nulls.to_string())
    before = len(df)
    df = df.dropna()
    dropped = before - len(df)
    if dropped:
        null_rate = dropped / before
        logger.warning("Dropped %d/%d rows with nulls (%.1f%%)", dropped, before, null_rate * 100)
        if null_rate > 0.10:
            raise ValueError(
                f"Null drop removed {null_rate:.1%} of rows — exceeds 10% threshold. "
                "Investigate transform_resale_flat_price for data quality issues."
            )
    else:
        logger.info("No nulls — dataset is complete")

    # One-hot encode categoricals
    df = pd.get_dummies(df, columns=CATEGORICAL_COLS, drop_first=False, dtype=int)

    X = df.drop(columns=[TARGET_COL])
    y = df[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, shuffle=True
    )
    logger.info("Train: %d rows | Test: %d rows", len(X_train), len(X_test))

    return X_train, X_test, y_train, y_test
