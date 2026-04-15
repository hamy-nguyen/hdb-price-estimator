import pandas as pd
import hashlib
import json
from decimal import Decimal
from datetime import datetime, date



FINGERPRINT_COL = "_fp"



def normalize_value(v):
    """
    Normalize values for stable hashing.
    """
    if pd.isna(v):
        return None
    if isinstance(v, float):
        return format(v, ".15g")   # deterministic float rendering
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (pd.Timestamp, datetime, date)):
            return v.isoformat()
    return v



def row_fingerprint(row: pd.Series, exclude_cols=None) -> str:
    """
    Generate SHA-256 fingerprint for a single row.
    """
    if exclude_cols is None:
        
        exclude_cols = set()

    # Build canonical dict with sorted keys
    record = {
        col: normalize_value(row[col])
        for col in sorted(row.index)
        if col not in exclude_cols
    }

    # Deterministic JSON encoding
    canonical = json.dumps(
        record,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False
    )

    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()



def add_fingerprint_column(df: pd.DataFrame, exclude_cols=None) -> pd.DataFrame:
    """
    Adds a fingerprint column to the dataframe.
    """
    df = df.copy()
    df[FINGERPRINT_COL] = df.apply(
        lambda row: row_fingerprint(row, exclude_cols=exclude_cols),
        axis=1
    )

    return df
