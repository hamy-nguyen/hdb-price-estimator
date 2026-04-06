"""
Derive the 16-field JSON body for the hosted HDB POST /predict API from postal code / address.

Flow (same idea as testing.ipynb):
  OneMap search → WGS84 on first hit → Haversine match to `transform_resale_flat_price`
  (`latitude` / `longitude`) → read **14** API fields from the closest row; **`flat_model`** and
  **`remaining_lease_years`** are supplied by the caller (e.g. Streamlit).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import mysql.connector
import numpy as np
import pandas as pd
import pyproj

# Backend config (not visible): MySQL database name for the data connection
MYSQL_DB = "HDB_Data"

R_EARTH_M = 6_371_000.0

# Hosted FastAPI inference (Hugging Face Space) — POST /predict — 16 features only
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
    "n_supermarket_within_1km",
]

HF_INT_KEYS = {
    "flat_model",
    "town",
    "n_mrt_within_1km",
    "n_bus_stop_within_1km",
    "month_index",
    "n_food_within_1km",
    "n_supermarket_within_1km",
}

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
    "Premium Apartment Loft",
)

HF_KEYS_FROM_ROW: tuple[str, ...] = tuple(
    k for k in HF_API_16_KEYS if k not in ("flat_model", "remaining_lease_years")
)


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
    "town": 10,
    "dist_to_nearest_mrt_m": 450.0,
    "n_mrt_within_1km": 2,
    "dist_to_nearest_bus_stop_m": 80.0,
    "n_bus_stop_within_1km": 15,
    "month_index": 120,
    "dist_to_food_m": 150.0,
    "n_food_within_1km": 10,
    "dist_to_supermarket_m": 250.0,
    "n_supermarket_within_1km": 3,
}

SG_CENTER = dict(lat=1.3521, lon=103.8198)
SG_BOUNDS = dict(west=103.6, east=104.1, south=1.15, north=1.48)


def _load_search_onemap():
    # Repo root is one level above this file (`web_application/predict_api_params.py`).
    repo_root = Path(__file__).resolve().parents[1]
    path = repo_root / "scripts" / "onemap_address_search.py"
    spec = importlib.util.spec_from_file_location("_onemap_address_search", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load OneMap search module from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.search_onemap


search_onemap = _load_search_onemap()


def load_data() -> pd.DataFrame:
    conn = mysql.connector.connect(
        host="localhost",
        user="airflow_user",
        password="password",
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


def _month_index_from_row(row: pd.Series, pool: pd.DataFrame) -> int:
    if "month_index" in row.index and pd.notna(row["month_index"]):
        try:
            return int(round(float(row["month_index"])))
        except (TypeError, ValueError):
            pass
    pm = _pool_scalar(pool, "month_index")
    if pm is not None:
        return int(round(pm))
    y = float(pool["year"].median()) if "year" in pool.columns else 2000.0
    mo = float(pool["month"].median()) if "month" in pool.columns else 1.0
    if "year" in row.index and pd.notna(row["year"]):
        try:
            y = float(row["year"])
        except (TypeError, ValueError):
            pass
    if "month" in row.index and pd.notna(row["month"]):
        try:
            mo = float(row["month"])
        except (TypeError, ValueError):
            pass
    return int((int(y) - 1990) * 12 + (int(mo) - 1))


def _value_for_key(row: pd.Series, pool: pd.DataFrame, key: str) -> float | int:
    default = HF_DEFAULTS[key]
    raw = row[key] if key in row.index else None
    if raw is None or (isinstance(raw, (float, np.floating)) and np.isnan(raw)) or pd.isna(raw):
        pv = _pool_scalar(pool, key)
        if pv is not None:
            raw = pv
        else:
            raw = default
    if key == "month_index":
        return _month_index_from_row(row, pool)
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
    flat_model: int,
    remaining_lease_years: float,
) -> dict[str, Any]:
    """Merge row-derived fields with caller-supplied `flat_model` and `remaining_lease_years`."""
    partial = build_hf_payload_from_row_match(matched_row, pool)
    partial["flat_model"] = int(flat_model)
    partial["remaining_lease_years"] = float(remaining_lease_years)
    return {k: partial[k] for k in HF_API_16_KEYS}


def build_hdb_predict_payload(
    postal_code: str,
    address: str | None = None,
    *,
    flat_model: int,
    remaining_lease_years: float,
    token: str | None = None,
    resale_df: pd.DataFrame | None = None,
    max_match_m: float = 75.0,
    widen_match_m: float | None = 500.0,
) -> dict[str, Any]:
    """
    OneMap → first hit WGS84 → nearest row in `transform_resale_flat_price` → 14 derived fields
    plus required **`flat_model`** and **`remaining_lease_years`** from the caller.

    Parameters
    ----------
    resale_df :
        Optional pre-loaded transform table; if None, `load_data()` is used.
    max_match_m / widen_match_m :
        Haversine radius (metres), same behaviour as testing.ipynb.
    """
    q = search_query_for_onemap(postal_code, address)
    raw: dict[str, Any] = search_onemap(q, token=token)
    search_df = onemap_response_to_dataframe(raw)

    if search_df.empty:
        raise ValueError("OneMap returned no results for this query.")

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
    if near.empty:
        raise ValueError(
            "No transform_resale_flat_price rows within search radius; "
            "check lat/lon columns or increase widen_match_m."
        )

    template = near.iloc[0].drop(labels=["_match_distance_m"], errors="ignore")
    return build_hf16_payload_from_row(
        template,
        resale_n,
        flat_model=flat_model,
        remaining_lease_years=remaining_lease_years,
    )
