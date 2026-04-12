# Imports — shared by all components (no direct UI)
import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
import requests

from predict_api_params import (
    STOREY_RANGE_OPTIONS,
    FLAT_MODEL_OPTIONS,
    PREDICT_API_URL,
    build_hdb_predict_payload,
    month_index_from_ym,
    sale_month_options,
)

# App shell: page title, layout, browser tab — affects the whole window
st.set_page_config(layout="wide", page_title="HDB Resale Price Prediction Dashboard")

# Main header — top of the page (large title text)
st.title("HDB Resale Price Prediction Dashboard")

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
# Price estimate — OneMap (postal) → nearest HDB row for location features → POST /predict
# User supplies flat details; location features come from the nearest resale row
# or dataset medians as fallback (so any Singapore address works).
# -----------------------------------------------------------------------------
st.subheader("Estimate Resale Price")
st.caption(
    f"Enter any Singapore postal code. **OneMap** resolves it to coordinates, then the app "
    f"looks up nearby HDB location data (MRT, bus, food distances). If no nearby HDB data "
    f"exists, dataset averages are used — the estimate still works but is less localised. "
    f"POSTs to **`{PREDICT_API_URL}`**."
)

_sale_months = sale_month_options()  # newest first, e.g. ["2026-03", "2026-02", ...]

with st.form("price_estimate_form"):
    col1, col2 = st.columns(2)
    with col1:
        postal_in = st.text_input("Postal code (6 digits)", placeholder="e.g. 200640")
        addr_hint = st.text_input("Address hint (Optional; Refines search)", value="")
        fm_label = st.selectbox("Flat model", options=list(FLAT_MODEL_OPTIONS))
        sale_month = st.selectbox("Month of sale", options=_sale_months, index=0)
    with col2:
        storey_label = st.selectbox(
            "Floor level",
            options=list(STOREY_RANGE_OPTIONS.keys()),
            index=3,  # default: Floor 10–12
        )
        floor_area = st.number_input(
            "Floor area (sqm)",
            min_value=28.0,
            max_value=280.0,
            value=90.0,
            step=1.0,
        )
        rly = st.number_input(
            "Remaining lease (years)",
            min_value=0.0,
            max_value=99.0,
            value=75.0,
            step=1.0,
        )
    submitted = st.form_submit_button("Get price estimate")

if submitted:
    digits = "".join(c for c in (postal_in or "") if c.isdigit())
    if len(digits) != 6:
        st.warning("Enter a 6-digit Singapore postal code.")
    else:
        storey_mid_val = STOREY_RANGE_OPTIONS[storey_label]
        sale_year, sale_mon = int(sale_month[:4]), int(sale_month[5:])
        sale_month_idx = month_index_from_ym(sale_year, sale_mon)
        try:
            payload, used_fallback = build_hdb_predict_payload(
                digits,
                addr_hint.strip() or None,
                flat_model=fm_label,
                floor_area_sqm=float(floor_area),
                storey_mid=float(storey_mid_val),
                remaining_lease_years=float(rly),
                month_index=sale_month_idx,
                resale_df=df,
            )
        except RuntimeError as e:
            st.error(f"OneMap authentication failed: {e}")
        except ValueError as e:
            st.error(str(e))
        except Exception as e:
            st.error(f"Could not build prediction payload: {e}")
        else:
            if used_fallback:
                st.info(
                    "No HDB resale transactions found near this address — "
                    "location features (MRT, bus, food distances) are estimated from "
                    "dataset averages. The prediction is approximate."
                )
            try:
                response = requests.post(PREDICT_API_URL, json=payload, timeout=120)
                response.raise_for_status()
                out = response.json()
            except requests.RequestException as e:
                st.error(f"API request failed: {e}")
            except ValueError as e:
                st.error(f"Invalid JSON from API: {e}")
            else:
                price = out.get("predicted_price")
                is_dummy = out.get("is_dummy")
                st.metric(
                    "Predicted resale price",
                    f"S$ {price:,.0f}" if isinstance(price, (int, float)) else str(price),
                )
                if is_dummy:
                    st.caption("Model not loaded — dummy prediction returned.")
                with st.expander("Request JSON sent to API"):
                    st.json(payload)

# Data table (collapsible) — main area: expander → scrollable grid of all rows
with st.expander("View raw table (all loaded rows)"):
    st.dataframe(df, use_container_width=True)

