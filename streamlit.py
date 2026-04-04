# Imports — shared by all components (no direct UI)
import streamlit as st
import mysql.connector
import numpy as np
import pandas as pd
import plotly.express as px
from sklearn.preprocessing import StandardScaler

# App shell: page title, layout, browser tab — affects the whole window
st.set_page_config(layout="wide", page_title="HDB resale dashboard")

# Main header — top of the page (large title text)
st.title("HDB resale dashboard")

# Backend config (not visible): MySQL database name for the data connection
MYSQL_DB = "HDB_Data"

# Map defaults (not visible): used only by the interactive map component below
SG_CENTER = dict(lat=1.3521, lon=103.8198)
SG_BOUNDS = dict(west=103.6, east=104.1, south=1.15, north=1.48)

# Data layer (no widget): loads rows for the dashboard; cached to speed reloads
@st.cache_data(ttl=60)
def load_data():
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


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.str.lower()
    return out

# Error states — main area: red callout if MySQL or load fails; stops the app
try:
    df = load_data()
except mysql.connector.Error as e:
    st.error(f"MySQL error: {e}")
    st.stop()
except Exception as e:
    st.error(f"Failed to load data: {e}")
    st.stop()

df = _normalize_columns(df)

# Schema guard — main area: red callout if required columns missing for map
required = {"latitude", "longitude", "resale_price", "month_and_year"}
missing = required - set(df.columns)
if missing:
    st.error(f"Missing columns in data: {sorted(missing)}. Found: {list(df.columns)}")
    st.stop()

# Data prep (no widget): normalize types for the map and time controls
df["month_and_year"] = pd.to_datetime(df["month_and_year"], errors="coerce")
df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
df["resale_price"] = pd.to_numeric(df["resale_price"], errors="coerce")

map_df = df.dropna(subset=["latitude", "longitude", "resale_price", "month_and_year"]).copy()

if map_df.empty:
    st.warning(
        "No rows with valid coordinates and prices. "
        "Run **data_transform** so `transform_resale_flat_price` includes coordinates and `resale_price`."
    )
    st.stop()

# Status banner — main area: green success line under the title
st.success(f"{len(df):,} rows loaded from `{MYSQL_DB}.transform_resale_flat_price`")

periods = sorted(map_df["month_and_year"].dropna().unique())
if not periods:
    st.warning("No valid `month_and_year` values.")
    st.stop()


def _label_period(ts) -> str:
    t = pd.Timestamp(ts)
    return t.strftime("%Y-%m")

# Month selection (logic only): session_state lets the map render above the slider
# while still using the slider value (updated each rerun after the user drags).
_SS_MONTH = "map_selected_month"
if _SS_MONTH not in st.session_state:
    st.session_state[_SS_MONTH] = periods[-1]
if st.session_state[_SS_MONTH] not in periods:
    st.session_state[_SS_MONTH] = periods[-1]

selected_month = st.session_state[_SS_MONTH]
sub = map_df[map_df["month_and_year"] == selected_month].copy()
if sub.empty:
    st.warning("No rows for the selected month.")
    st.stop()

time_caption = _label_period(selected_month)
colorbar_title = "S$ (this month)"

# Map figure build (no widget yet): Plotly trace for Singapore scatter map
hover_cols = [c for c in ("full_address", "town", "flat_type", "floor_area_sqm", "storey_range") if c in sub.columns]
scatter_kw = dict(
    data_frame=sub,
    lat="latitude",
    lon="longitude",
    color="resale_price",
    color_continuous_scale=[
        [0.0, "#008000"],
        [0.5, "#ffcc00"],
        [1.0, "#cc0000"],
    ],
    zoom=10.5,
    height=440,
    labels={"resale_price": "Resale price (S$)"},
)
if "full_address" in sub.columns:
    scatter_kw["hover_name"] = "full_address"
if hover_cols:
    scatter_kw["hover_data"] = {c: True for c in hover_cols}

fig = px.scatter_map(**scatter_kw)

fig.update_layout(
    map=dict(
        center=SG_CENTER,
        bounds=SG_BOUNDS,
    ),
    margin=dict(l=0, r=0, t=40, b=0),
    coloraxis_colorbar=dict(title=colorbar_title),
)

# Interactive map — main area: pan/zoom map; hover tooltips; legend = price scale
st.plotly_chart(fig, use_container_width=True)

# Month scrubber — main area: draggable track under the map (updates map on release)
st.select_slider(
    "Month (drag along the track; green → red = cheapest → priciest in that month)",
    options=periods,
    key=_SS_MONTH,
    format_func=_label_period,
)

# Map footnote — main area: small text under the month slider
st.caption(
    f"Showing **{len(sub):,}** transactions for **{time_caption}**. "
    "Colour range is min→max resale price for that month only."
)

# -----------------------------------------------------------------------------
# Price estimate (placeholder) — main area: form below map/slider, above raw table.
# Until an ML model exists, "prediction" = resale_price of the closest historical row
# in the same month_and_year (numeric features: scaled Euclidean; categoricals: mismatch penalty).
# -----------------------------------------------------------------------------
PRED_NUM = [
    "floor_area_sqm",
    "dist_to_nearest_mrt_m",
    "dist_to_school_m",
    "dist_to_mall_m",
    "building_age",
]
PRED_CAT = ["town", "flat_type", "flat_model"]

st.subheader("Estimated resale price (nearest match)")
st.caption(
    "Uses the **same month** as the slider above. "
    "Later this can be swapped for a trained model; for now the estimate is the **actual `resale_price`** "
    "of the most similar listing in that month."
)

pred_pool = df[df["month_and_year"] == selected_month].copy()
for c in PRED_NUM:
    if c in pred_pool.columns:
        pred_pool[c] = pd.to_numeric(pred_pool[c], errors="coerce")

if pred_pool.empty:
    st.info("No rows in this month to run a nearest match.")
else:

    def _nearest_match(
        pool: pd.DataFrame,
        num_inputs: dict[str, float | None],
        town: str | None,
        flat_type: str | None,
        flat_model: str | None,
        cat_weight: float,
    ) -> tuple[pd.Series, float, list[str]]:
        """Return best row, distance, and list of numeric columns used."""
        work = pool.copy()
        used_num: list[str] = []
        X_parts = []
        for col in PRED_NUM:
            if col not in work.columns:
                continue
            if num_inputs.get(col) is None:
                continue
            col_series = work[col]
            med = float(col_series.median())
            if np.isnan(med):
                continue
            filled = col_series.fillna(med)
            val = float(num_inputs[col])
            used_num.append(col)
            X_parts.append(filled.to_numpy(dtype=float).reshape(-1, 1))
        if not used_num:
            dist_num = np.zeros(len(work))
        else:
            X = np.hstack(X_parts)
            u_row = np.array([[num_inputs[c] for c in used_num]], dtype=float)
            scaler = StandardScaler()
            Xz = scaler.fit_transform(X)
            uz = scaler.transform(u_row)
            dist_num = np.linalg.norm(Xz - uz, axis=1)

        pen = np.zeros(len(work))
        for col, raw in (("town", town), ("flat_type", flat_type), ("flat_model", flat_model)):
            if not raw or str(raw).strip() == "" or col not in work.columns:
                continue
            want = str(raw).strip().upper()
            actual = work[col].astype(str).str.strip().str.upper()
            pen += (actual != want).astype(float) * cat_weight

        total = dist_num + pen
        j = int(np.argmin(total))
        return work.iloc[j], float(total[j]), used_num

    towns = sorted(pred_pool["town"].dropna().astype(str).unique()) if "town" in pred_pool.columns else []
    ftypes = sorted(pred_pool["flat_type"].dropna().astype(str).unique()) if "flat_type" in pred_pool.columns else []
    fmodels = sorted(pred_pool["flat_model"].dropna().astype(str).unique()) if "flat_model" in pred_pool.columns else []

    with st.form("price_estimate_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            town_v = st.selectbox("Town", options=[""] + towns, format_func=lambda x: "(any)" if x == "" else x)
        with c2:
            ft_v = st.selectbox("Flat type", options=[""] + ftypes, format_func=lambda x: "(any)" if x == "" else x)
        with c3:
            fm_v = st.selectbox("Flat model", options=[""] + fmodels, format_func=lambda x: "(any)" if x == "" else x)

        c4, c5 = st.columns(2)
        with c4:
            fa = st.number_input(
                "Floor area (sqm)",
                min_value=0.0,
                max_value=500.0,
                value=float(pred_pool["floor_area_sqm"].median()) if "floor_area_sqm" in pred_pool.columns else 90.0,
                step=1.0,
            )
        with c5:
            ba = st.number_input(
                "Building age (years)",
                min_value=0.0,
                max_value=120.0,
                value=float(pred_pool["building_age"].median()) if "building_age" in pred_pool.columns else 20.0,
                step=1.0,
            )

        st.markdown("**Distances (m)** — leave blank to omit from matching (not treated as zero).")
        c6, c7, c8 = st.columns(3)
        with c6:
            d_mrt = st.text_input("Nearest MRT", value="", placeholder="e.g. 450")
        with c7:
            d_sch = st.text_input("Nearest school", value="", placeholder="e.g. 300")
        with c8:
            d_mall = st.text_input("Nearest mall", value="", placeholder="e.g. 600")

        cat_w = st.slider("Strictness on town / type / model (higher = must match more closely)", 0.0, 5.0, 2.0, 0.5)

        submitted = st.form_submit_button("Find closest listing & show proxy price")

    if submitted:

        def _parse_opt_float(s: str) -> float | None:
            s = (s or "").strip()
            if s == "":
                return None
            try:
                return float(s)
            except ValueError:
                st.warning(f"Ignored non-numeric distance: {s!r}")
                return None

        num_in: dict[str, float | None] = {
            "floor_area_sqm": float(fa),
            "building_age": float(ba),
            "dist_to_nearest_mrt_m": _parse_opt_float(d_mrt),
            "dist_to_school_m": _parse_opt_float(d_sch),
            "dist_to_mall_m": _parse_opt_float(d_mall),
        }

        try:
            best, dist, used_cols = _nearest_match(
                pred_pool,
                num_in,
                town_v or None,
                ft_v or None,
                fm_v or None,
                cat_weight=cat_w,
            )
        except Exception as e:
            st.error(f"Matching failed: {e}")
            best = None

        if best is not None:
            price = best.get("resale_price", np.nan)
            st.metric(
                "Proxy recommended price (current: nearest neighbour `resale_price`)",
                f"S$ {price:,.0f}" if pd.notna(price) else "n/a",
            )
            st.caption(
                f"Distance score ≈ **{dist:.3f}** (lower is closer). "
                f"Numeric features used: **{', '.join(used_cols) or 'none (categorical only)'}**."
            )
            show_cols = [
                c
                for c in (
                    "full_address",
                    "town",
                    "flat_type",
                    "flat_model",
                    "floor_area_sqm",
                    "storey_range",
                    "resale_price",
                    "month_and_year",
                    "dist_to_nearest_mrt_m",
                    "dist_to_school_m",
                    "dist_to_mall_m",
                    "building_age",
                )
                if c in best.index
            ]
            st.dataframe(best[show_cols].to_frame(name="value"), use_container_width=True)

# Data table (collapsible) — main area: expander → scrollable grid of all rows
with st.expander("View raw table (all loaded rows)"):
    st.dataframe(df, use_container_width=True)

