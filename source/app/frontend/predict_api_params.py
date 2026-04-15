"""
Derive the 16-field JSON body for the hosted HDB POST /predict API from postal code / address.

Flow (same idea as testing.ipynb):
  OneMap search → WGS84 on first hit → Haversine match to `transform_resale_flat_price`
  (`latitude` / `longitude`) → read **14** API fields from the closest row; **`flat_model`** and
  **`remaining_lease_years`** are supplied by the caller (e.g. Streamlit).
"""

from __future__ import annotations

import importlib.util
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import mysql.connector
import numpy as np
import pandas as pd
import pyproj
from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parents[3]
# override=True: repo `.env` wins over a stale `export PREDICT_API_URL=http://localhost:7860/predict` in the shell.
load_dotenv(_REPO_ROOT / ".env", override=True)

# MySQL for map / OneMap matching (same as README: MYSQL_* in project `.env`)
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "bt4301")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "password")
MYSQL_DB = os.getenv("MYSQL_DATABASE", "HDB_Data")

R_EARTH_M = 6_371_000.0

# Hosted POST /predict (change here to point at another API, e.g. local http://127.0.0.1:7860/predict).
PREDICT_API_URL = "https://hamynguyen-hdb-price-estimator.hf.space/predict"

HF_API_16_KEYS = [
    "flat_model",
    "floor_area_sqm",
    "max_floor_lvl",
    "total_dwelling_units",
    "storey_mid",
    "remaining_lease_years",
    "town",
    "dist_to_nearest_mrt_m",
    "n_mrt_within_1km",
    "dist_to_nearest_bus_stop_m",
    "n_bus_stop_within_1km",
    "month_index",
    "dist_to_food_m",
    "n_food_within_1km",
    "dist_to_supermarket_m",
    "n_supermarket_within_1km"
]

HF_INT_KEYS = {
    # flat_model and town are strings (e.g. 'Apartment', 'Jurong West') — excluded.
    "n_mrt_within_1km",
    "n_bus_stop_within_1km",
    "month_index",
    "n_food_within_1km",
    "n_supermarket_within_1km"
}

# Keys whose values must be passed as raw strings (read directly from the transform row).
HF_STR_KEYS = {"town"}

# UI / API encoding: integer code = index in this tuple (must match training label order if used).
FLAT_MODEL_OPTIONS: tuple[str, ...] = (
    "Improved",
    "New Generation",
    "Model A",
    "DBSS",
    "Standard",
    "Apartment",
    "Adjoined flat",
    "Maisonette",
    "Simplified",
    "Premium Apartment",
    "Model A-Maisonette",
    "2-room",
    "Multi Generation",
    "Model A2",
    "3Gen",
    "Type S1",
    "Type S2",
    "Premium Maisonette",
    "Improved-Maisonette",
    "Terrace",
    "Premium Apartment Loft"
)

# Keys derived from the nearest transform row (location / building-level features).
# flat_model, remaining_lease_years, storey_mid, floor_area_sqm, month_index are user-provided.
HF_KEYS_FROM_ROW: tuple[str, ...] = tuple(
    k for k in HF_API_16_KEYS
    if k not in ("flat_model", "remaining_lease_years", "storey_mid", "floor_area_sqm", "month_index")
)


DATASET_BASE_YEAR = 2017
DATASET_BASE_MONTH = 1  # earliest resale transaction in transform_resale_flat_price


def month_index_from_ym(year: int, month: int) -> int:
    """
    Convert a year/month to the month_index feature used during training.
    Formula mirrors transform_dag_helpers.py:
        (year - min_year) * 12 + (month - min_month)
    where the minimum month in the dataset is 2017-01 → index 0.
    """
    return (year - DATASET_BASE_YEAR) * 12 + (month - DATASET_BASE_MONTH)


def sale_month_options() -> list[str]:
    """
    Return month labels from Jan 2017 up to (and including) last month,
    formatted as 'YYYY-MM', newest first.
    """
    now = datetime.now()
    # Last month
    if now.month == 1:
        end_year, end_month = now.year - 1, 12
    else:
        end_year, end_month = now.year, now.month - 1

    options = []
    y, m = end_year, end_month
    while (y, m) >= (2017, 1):
        options.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return options

# Standard HDB storey ranges → midpoint value used as the storey_mid feature.
# Label shown in the UI → storey_mid sent to the model.
STOREY_RANGE_OPTIONS: dict[str, int] = {
    "Floor 1-3":   2,  "Floor 4-6":   5,  "Floor 7-9":   8,
    "Floor 10-12": 11, "Floor 13-15": 14, "Floor 16-18": 17,
    "Floor 19-21": 20, "Floor 22-24": 23, "Floor 25-27": 26,
    "Floor 28-30": 29, "Floor 31-33": 32, "Floor 34-36": 35,
    "Floor 37-39": 38, "Floor 40-42": 41, "Floor 43-45": 44,
    "Floor 46-48": 47, "Floor 49-51": 50
}


def flat_model_option_to_int(label: str) -> int:
    """Map dropdown label → integer code for POST /predict (`flat_model`)."""
    s = str(label).strip()
    try:
        return FLAT_MODEL_OPTIONS.index(s)
    except ValueError as e:
        raise ValueError(
            f"Unknown flat_model {label!r}; expected one of FLAT_MODEL_OPTIONS ({len(FLAT_MODEL_OPTIONS)} labels)."
        ) from e


# Defaults when row and pool both lack a value (flat_model / remaining_lease_years: user-provided only)
HF_DEFAULTS: dict[str, float | int] = {
    "floor_area_sqm": 90.0,
    "max_floor_lvl": 15.0,
    "total_dwelling_units": 200.0,
    "storey_mid": 8.0,
    "town": "Jurong West",
    "dist_to_nearest_mrt_m": 450.0,
    "n_mrt_within_1km": 2,
    "dist_to_nearest_bus_stop_m": 80.0,
    "n_bus_stop_within_1km": 15,
    "dist_to_food_m": 150.0,
    "n_food_within_1km": 10,
    "dist_to_supermarket_m": 250.0,
    "n_supermarket_within_1km": 3,
}

SG_CENTER = dict(lat=1.3521, lon=103.8198)
SG_BOUNDS = dict(west=103.6, east=104.1, south=1.15, north=1.48)


def _load_search_onemap():
    path = Path(__file__).resolve().parents[1] / "scripts" / "onemap_address_search.py"
    spec = importlib.util.spec_from_file_location("_onemap_address_search", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load OneMap search module from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.search_onemap


search_onemap = _load_search_onemap()


def load_data() -> pd.DataFrame:
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
    )
    try:
        return pd.read_sql("SELECT * FROM transform_resale_flat_price", con=conn)
    finally:
        conn.close()


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.str.lower()
    return out


def add_wgs84_to_onemap_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    n = len(out)
    lat_num = (
        pd.to_numeric(out["LATITUDE"], errors="coerce")
        if "LATITUDE" in out.columns
        else pd.Series([float("nan")] * n, index=out.index, dtype="float64")
    )
    if "LONGITUDE" in out.columns:
        lon_num = pd.to_numeric(out["LONGITUDE"], errors="coerce")
    elif "LONGTITUDE" in out.columns:
        lon_num = pd.to_numeric(out["LONGTITUDE"], errors="coerce")
    else:
        lon_num = pd.Series([float("nan")] * n, index=out.index, dtype="float64")

    need = lat_num.isna() | lon_num.isna()
    if need.any() and "X" in out.columns and "Y" in out.columns:
        x = pd.to_numeric(out["X"], errors="coerce")
        y = pd.to_numeric(out["Y"], errors="coerce")
        sub = need & x.notna() & y.notna()
        if sub.any():
            t = pyproj.Transformer.from_crs("EPSG:3414", "EPSG:4326", always_xy=True)
            lon_fix, lat_fix = t.transform(
                x.loc[sub].to_numpy(dtype=float),
                y.loc[sub].to_numpy(dtype=float),
            )
            lat_num = lat_num.copy()
            lon_num = lon_num.copy()
            lat_num.loc[sub] = lat_fix
            lon_num.loc[sub] = lon_fix

    out["latitude_wgs84"] = lat_num
    out["longitude_wgs84"] = lon_num
    return out


def onemap_response_to_dataframe(data: dict[str, Any]) -> pd.DataFrame:
    rows = data.get("results") or []
    if not rows:
        return pd.DataFrame()
    return add_wgs84_to_onemap_df(pd.json_normalize(rows))


def search_query_for_onemap(postal_code: str, address: str | None = None) -> str:
    a = (address or "").strip()
    if a:
        return a
    digits = "".join(c for c in postal_code if c.isdigit())
    if len(digits) != 6:
        raise ValueError(
            f"Need a non-empty address or a 6-digit postal code; got postal_code={postal_code!r}"
        )
    return digits


def haversine_m(lat1: float, lon1: float, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dl = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dl / 2) ** 2
    return 2 * R_EARTH_M * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def find_resale_rows_near_wgs84(
    resale_df: pd.DataFrame,
    lat_wgs84: float,
    lng_wgs84: float,
    *,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
    max_distance_m: float = 75.0,
    widen_to_m: float | None = 500.0,
) -> pd.DataFrame:
    """
    Same Haversine logic as testing.ipynb: keep rows within max_distance_m of the OneMap point.
    If none, optionally retry with widen_to_m.
    """
    for c in (lat_col, lon_col):
        if c not in resale_df.columns:
            raise KeyError(
                f"Missing {c!r} in resale data; columns: "
                f"{[x for x in resale_df.columns if 'lat' in x or 'lon' in x or 'lng' in x]}"
            )

    def _query(max_m: float) -> pd.DataFrame:
        dlat = pd.to_numeric(resale_df[lat_col], errors="coerce")
        dlon = pd.to_numeric(resale_df[lon_col], errors="coerce")
        ok = dlat.notna() & dlon.notna()
        ok_arr = ok.to_numpy()
        dist = np.full(len(resale_df), np.nan)
        dist[ok_arr] = haversine_m(lat_wgs84, lng_wgs84, dlat[ok].to_numpy(), dlon[ok].to_numpy())
        mask = ok_arr & (dist <= max_m)
        out = resale_df.loc[mask].copy()
        out["_match_distance_m"] = dist[mask]
        return out.sort_values("_match_distance_m")

    hits = _query(max_distance_m)
    if len(hits) == 0 and widen_to_m is not None and widen_to_m > max_distance_m:
        hits = _query(widen_to_m)
    return hits


def _pool_scalar(pool: pd.DataFrame, key: str) -> float | None:
    if key not in pool.columns:
        return None
    s = pd.to_numeric(pool[key], errors="coerce")
    m = s.median()
    return float(m) if pd.notna(m) else None


def _value_for_key(row: pd.Series, pool: pd.DataFrame, key: str) -> float | int | str:
    default = HF_DEFAULTS[key]
    raw = row[key] if key in row.index else None
    if raw is None or (isinstance(raw, (float, np.floating)) and np.isnan(raw)) or pd.isna(raw):
        pv = _pool_scalar(pool, key)
        if pv is not None:
            raw = pv
        else:
            raw = default
    if key in HF_STR_KEYS:
        return str(raw)
    if key in HF_INT_KEYS:
        try:
            return int(round(float(raw)))
        except (TypeError, ValueError):
            try:
                return int(round(float(default)))
            except (TypeError, ValueError):
                return int(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def build_hf_payload_from_row_match(matched_row: pd.Series, pool: pd.DataFrame) -> dict[str, Any]:
    """Closest transform row → the 14 hosted-API fields that are **not** user-provided."""
    row = matched_row.copy()
    row.index = row.index.str.lower()
    pool_l = normalize_dataframe_columns(pool)
    return {k: _value_for_key(row, pool_l, k) for k in HF_KEYS_FROM_ROW}


def build_hf16_payload_from_row(
    matched_row: pd.Series,
    pool: pd.DataFrame,
    *,
    flat_model: str,
    floor_area_sqm: float,
    storey_mid: float,
    remaining_lease_years: float,
    month_index: int,
) -> dict[str, Any]:
    """Merge location features from the nearest row with all user-supplied flat fields."""
    partial = build_hf_payload_from_row_match(matched_row, pool)
    partial["flat_model"] = str(flat_model)
    partial["floor_area_sqm"] = float(floor_area_sqm)
    partial["storey_mid"] = float(storey_mid)
    partial["remaining_lease_years"] = float(remaining_lease_years)
    partial["month_index"] = int(month_index)
    return {k: partial[k] for k in HF_API_16_KEYS}


def _pool_only_payload(
    pool: pd.DataFrame,
    *,
    flat_model: str,
    floor_area_sqm: float,
    storey_mid: float,
    remaining_lease_years: float,
    month_index: int,
) -> dict[str, Any]:
    """
    Fallback when no nearby row exists: build payload from dataset-wide medians
    for all location features, combined with the user-supplied flat fields.
    """
    empty_row = pd.Series(dtype=object)
    pool_l = normalize_dataframe_columns(pool)
    partial = {k: _value_for_key(empty_row, pool_l, k) for k in HF_KEYS_FROM_ROW}
    partial["flat_model"] = str(flat_model)
    partial["floor_area_sqm"] = float(floor_area_sqm)
    partial["storey_mid"] = float(storey_mid)
    partial["remaining_lease_years"] = float(remaining_lease_years)
    partial["month_index"] = int(month_index)
    return {k: partial[k] for k in HF_API_16_KEYS}


def build_hdb_predict_payload(
    postal_code: str,
    address: str | None = None,
    *,
    flat_model: str,
    floor_area_sqm: float,
    storey_mid: float,
    remaining_lease_years: float,
    month_index: int,
    token: str | None = None,
    resale_df: pd.DataFrame | None = None,
    max_match_m: float = 75.0,
    widen_match_m: float | None = 2000.0,
) -> tuple[dict[str, Any], bool]:
    """
    OneMap → WGS84 → nearest HDB row for location features → 16-field predict payload.

    Location features (MRT/bus/food distances etc.) come from the closest resale
    transaction within widen_match_m.  If nothing is found at any radius, dataset-wide
    medians are used as a fallback so that any valid Singapore address works.

    Flat-specific fields (flat_model, floor_area_sqm, storey_mid, remaining_lease_years)
    always come from the caller.

    Returns
    -------
    payload : dict[str, Any]   — 16-field body ready for POST /predict
    used_fallback : bool       — True when dataset medians were used (no nearby row found)
    """
    q = search_query_for_onemap(postal_code, address)
    raw: dict[str, Any] = search_onemap(q, token=token)
    search_df = onemap_response_to_dataframe(raw)

    if search_df.empty:
        raise ValueError("OneMap returned no results for this postal code / address.")

    lat0 = float(pd.to_numeric(search_df.iloc[0]["latitude_wgs84"], errors="coerce"))
    lon0 = float(pd.to_numeric(search_df.iloc[0]["longitude_wgs84"], errors="coerce"))
    if np.isnan(lat0) or np.isnan(lon0):
        raise ValueError("OneMap hit has no usable WGS84 coordinates.")

    resale = resale_df if resale_df is not None else load_data()
    resale_n = normalize_dataframe_columns(resale)

    near = find_resale_rows_near_wgs84(
        resale_n,
        lat0,
        lon0,
        lat_col="latitude",
        lon_col="longitude",
        max_distance_m=max_match_m,
        widen_to_m=widen_match_m,
    )

    user_fields = dict(
        flat_model=flat_model,
        floor_area_sqm=floor_area_sqm,
        storey_mid=storey_mid,
        remaining_lease_years=remaining_lease_years,
        month_index=month_index,
    )

    if near.empty:
        # No HDB transactions found near this address — use dataset medians for
        # location features.  Prediction is still valid but less localised.
        payload = _pool_only_payload(resale_n, **user_fields)
        return payload, True

    template = near.iloc[0].drop(labels=["_match_distance_m"], errors="ignore")
    payload = build_hf16_payload_from_row(template, resale_n, **user_fields)
    return payload, False
