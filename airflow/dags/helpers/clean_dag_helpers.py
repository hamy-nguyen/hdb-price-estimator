"""
Cleaning helpers for the data_clean DAG.

Each function:
  1. Reads one raw_* table from MySQL into a DataFrame
  2. Applies cleaning logic (fill in your logic in each TODO section)
  3. Writes the result to a clean_* table (replace each run)

All functions accept mysql_conn_id and use the shared get_mysql_engine helper
so connection details stay in Airflow's connection store, not in code.
"""

import gc
import logging

import pandas as pd
import re
import numpy as np
from sqlalchemy import create_engine, text
from pyproj import Transformer

from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def get_mysql_engine(mysql_conn_id: str):
    conn = BaseHook.get_connection(mysql_conn_id)
    return create_engine(
        f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )

def _read(sql: str, engine) -> pd.DataFrame:
    """Read a SQL query into a DataFrame using an explicit connection (pandas 2.x + SQLAlchemy 2.x compatible)."""
    with engine.connect() as conn:
        return pd.read_sql(text(sql), con=conn)

def _write(df: pd.DataFrame, table: str, engine) -> None:
    """Write a DataFrame to MySQL, replacing any existing table."""
    df.to_sql(table, con=engine, if_exists="replace", index=False)
    logger.info("Wrote %d rows to %s", len(df), table)

# ---------------------------------------------------------------------------
# One function per raw table
# ---------------------------------------------------------------------------

def clean_hdb(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_hdb...")
    engine = get_mysql_engine(mysql_conn_id)

    hdb = _read("SELECT * FROM raw_hdb", engine)

    # Drop "unnamed:_0" column
    hdb.drop(columns=['unnamed:_0'], errors='ignore', inplace=True)

    # Standardize text
    hdb['street'] = hdb['street'].str.upper()
    hdb['building'] = hdb['building'].str.upper()

    # rename columns for better readability
    hdb = hdb.rename(columns={
        'pln_area_n': 'planning_area',
        'region_n': 'region'
    })

    # Convert columns to the right datatype
    hdb["total_dwelling_units"] = pd.to_numeric(hdb["total_dwelling_units"], errors="coerce")
    hdb['lat'] = pd.to_numeric(hdb['lat'], errors='coerce')
    hdb['lng'] = pd.to_numeric(hdb['lng'], errors='coerce')
    hdb['max_floor_lvl'] = pd.to_numeric(hdb['max_floor_lvl'], errors='coerce')
    hdb['year_completed'] = pd.to_numeric(hdb['year_completed'], errors='coerce')

    # Remove missing values
    hdb = hdb.dropna(subset=['total_dwelling_units', 'lat', 'lng', 'max_floor_lvl', 'year_completed'])

    # Remove negative dwelling values
    hdb = hdb[hdb['total_dwelling_units'] > 0]

    # Remove duplicates
    hdb = hdb.drop_duplicates(subset=['blk_no','street'])

    _write(hdb, "clean_hdb", engine)

    engine.dispose()
    del hdb
    gc.collect()

def clean_mrt(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_mrt...")
    engine = get_mysql_engine(mysql_conn_id)

    mrt = _read("SELECT * FROM raw_mrt", engine)

    # Drop "unnamed:_0" column
    mrt.drop(columns=['unnamed:_0'], errors='ignore', inplace=True)

    # Standardize text
    mrt['name'] = mrt['name'].str.upper()

    # Convert columns to the right datatype
    mrt["lat"] = pd.to_numeric(mrt["lat"], errors='coerce')
    mrt["lng"] = pd.to_numeric(mrt["lng"], errors='coerce')

    # Remove missing coordinates
    mrt = mrt.dropna(subset=['lat','lng'])

    # Remove duplicates
    mrt = mrt.drop_duplicates(subset=['stop_id'])

    _write(mrt, "clean_mrt", engine)

    engine.dispose()
    del mrt
    gc.collect()

def clean_poi(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_poi...")
    engine = get_mysql_engine(mysql_conn_id)

    poi = _read("SELECT * FROM raw_poi", engine)

    # Drop "unnamed:_0" column
    poi.drop(columns=['unnamed:_0'], errors='ignore', inplace=True)

    # Dropping other not so helpful columns
    poi.drop(columns=['price_level', 'brand', 'formatted_address', 'global_code'], inplace=True)

    # Drop rows with small missing location values (Just 2 rows)
    poi = poi.dropna(subset=[
        'pln_area_c', 'subzone_no', 'subzone_n',
        'subzone_c', 'pln_area_n', 'planning_area'
    ])

    # Remove duplicates
    poi = poi.drop_duplicates(subset=["place_id"])

    # Convert columns to the right datatype
    for col in ("lat", "lng", "rating", "user_ratings_total"):
        if col in poi.columns:
            poi[col] = pd.to_numeric(poi[col], errors="coerce")

    poi = poi.dropna(subset=["lat", "lng", "rating", "user_ratings_total"])

    # Validation: ratings should be between 0 and 5, user ratings total should be non-negative
    poi = poi[poi["rating"].between(0, 5)]
    poi = poi[poi["user_ratings_total"] >= 0]

    _write(poi, "clean_poi", engine)

    engine.dispose()
    del poi
    gc.collect()

def clean_onemap(mysql_conn_id: str) -> None:
    logger.info("Cleaning OneMap data...")
    engine = get_mysql_engine(mysql_conn_id)

    # transport_school
    transport_school = _read("SELECT * FROM raw_onemap_transport_school", engine)

    transport_cols = [
        'bus', 'mrt', 'mrt_bus', 'mrt_car',
        'mrt_other', 'taxi', 'car', 'pvt_chartered_bus', 'lorry_pickup',
        'motorcycle_scooter', 'others', 'no_transport_required',
        'other_combi_mrt_or_bus', 'mrt_lrt_only', 'mrt_lrt_and_bus',
        'other_combi_mrt_lrt_or_bus', 'taxi_pvt_hire_car_only',
        'pvt_chartered_bus_van'
    ]

    transport_school[transport_cols] = transport_school[transport_cols].apply(
        pd.to_numeric, errors='coerce'
    )
    # fill nas with 0s for transport_school as missing values likely indicate 0 transport demand
    transport_school = transport_school.fillna(0)

    _write(transport_school, "clean_onemap_transport_school", engine)
    
    del transport_school
    gc.collect()

    # transport_work
    transport_work = _read("SELECT * FROM raw_onemap_transport_work", engine)

    transport_work[transport_cols] = transport_work[transport_cols].apply(
        pd.to_numeric, errors='coerce'
    )
    transport_work = transport_work.fillna(0)

    _write(transport_work, "clean_onemap_transport_work", engine)

    del transport_work
    gc.collect()

    # tenancy
    tenancy = _read("SELECT * FROM raw_onemap_tenancy", engine)

    tenant_cols = ['owner', 'tenant', 'others']

    tenancy[tenant_cols] = tenancy[tenant_cols].apply(pd.to_numeric, errors='coerce')
    tenancy = tenancy.fillna(0)

    _write(tenancy, "clean_onemap_tenancy", engine)

    del tenancy
    gc.collect()

    # dwelling
    dwelling = _read("SELECT * FROM raw_onemap_dwelling", engine)

    dwelling_cols = [
        'hdb_1_and_2_room_flats',
        'hdb_3_room_flats',
        'hdb_4_room_flats',
        'hdb_5_room_and_executive_flats',
        'condominiums_and_other_apartments',
        'landed_properties',
        'others'
    ]

    dwelling[dwelling_cols] = dwelling[dwelling_cols].apply(
        pd.to_numeric, errors='coerce'
    )
    dwelling = dwelling.fillna(0)

    _write(dwelling, "clean_onemap_dwelling", engine)

    engine.dispose()
    del dwelling
    gc.collect()

def clean_carpark(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_carpark...")
    engine = get_mysql_engine(mysql_conn_id)

    car_park = _read("SELECT * FROM raw_carpark", engine)

    # Convert columns to the right datatype
    car_park['x_coord'] = pd.to_numeric(car_park['x_coord'], errors='coerce')
    car_park['y_coord'] = pd.to_numeric(car_park['y_coord'], errors='coerce')
    car_park['car_park_decks'] = pd.to_numeric(car_park['car_park_decks'], errors='coerce')
    car_park['gantry_height'] = pd.to_numeric(car_park['gantry_height'], errors='coerce')

    # Convert coordinates from SVY21 to WGS84
    transformer = Transformer.from_crs("EPSG:3414", "EPSG:4326")
    car_park["lat"], car_park["lng"] = transformer.transform(
        car_park["y_coord"].values,
        car_park["x_coord"].values
    )

    # drop original coordinate columns
    car_park = car_park.drop(columns=['x_coord', 'y_coord'], errors='ignore')

    # Fix values that are outside of Singapore's Latitude and Longitude Range
    SG_LAT = (1.15, 1.47)
    SG_LNG = (103.6, 104.1)

    outlier_coords = (
        (car_park["lat"] < SG_LAT[0]) | (car_park["lat"] > SG_LAT[1]) |
        (car_park["lng"] < SG_LNG[0]) | (car_park["lng"] > SG_LNG[1])
    )
    car_park.loc[outlier_coords, ["lat", "lng"]] = None

    # Drop rows with missing values
    car_park = car_park.dropna()

    # Drop duplicates in the dataset
    car_park = car_park.drop_duplicates()

    # Standardise categorical columns
    categorical_columns_for_standardisation = [
        "car_park_type", "type_of_parking_system", "short_term_parking", "free_parking"
    ]

    for col in categorical_columns_for_standardisation:
        car_park[col] = car_park[col].str.strip().str.upper()

    # Reset index
    car_park = car_park.reset_index(drop=True)

    # Handle outliers
    carpark_numeric_columns = ["car_park_decks", "gantry_height"]
    for num_col in carpark_numeric_columns:
        q1 = car_park[num_col].quantile(0.25)
        q3 = car_park[num_col].quantile(0.75)
        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # Identify outliers
        outlier_mask = (
            (car_park[num_col] < lower_bound) |
            (car_park[num_col] > upper_bound)
        )

        if outlier_mask.sum() > 0:
            # Median (exclude surface car parks for cleaner median)
            median_val = car_park.loc[
                car_park["car_park_type"] != "SURFACE CAR PARK", num_col
            ].median()

            # Surface car parks: set to 0
            surface_mask = (
                outlier_mask &
                (car_park["car_park_type"] == "SURFACE CAR PARK")
            )

            car_park.loc[surface_mask, num_col] = 0

            # Non-surface: set to median
            nonsurface_mask = (
                outlier_mask &
                (car_park["car_park_type"] != "SURFACE CAR PARK")
            )

            car_park.loc[nonsurface_mask, num_col] = median_val

    _write(car_park, "clean_carpark", engine)

    engine.dispose()
    del car_park
    gc.collect()

def clean_bus(mysql_conn_id: str) -> None:
    logger.info("Joining bus data...") 
    engine = get_mysql_engine(mysql_conn_id)

    # bus_stops
    bus_stops = _read("SELECT * FROM raw_bus_stops", engine)

    # Drop "unnamed:_0" column
    bus_stops.drop(columns=['unnamed:_0'], errors='ignore', inplace=True)

    bus_stops.rename(columns={
        'busstopcode': 'stop_id',
        'latitude': 'lat',
        'longitude': 'lng'
    }, inplace=True)

    bus_stops['stop_id'] = bus_stops['stop_id'].astype(str)
    bus_stops['lat'] = pd.to_numeric(bus_stops['lat'], errors="coerce")
    bus_stops['lng'] = pd.to_numeric(bus_stops['lng'], errors="coerce")
    bus_stops = bus_stops.dropna(subset=['lat','lng'])

    bus_stops = bus_stops.drop_duplicates(subset=['stop_id'])

    _write(bus_stops, "clean_bus_stops", engine)

    del bus_stops
    gc.collect()
    
    # bus_vol
    bus_vol = _read("SELECT * FROM raw_bus_vol", engine)

    # Drop "unnamed:_0" column
    bus_vol.drop(columns=['unnamed:_0'], errors='ignore', inplace=True)

    # Standardize stop_id to string format
    bus_vol['stop_id'] = bus_vol['stop_id'].astype(str)
    # data type conversion
    bus_vol["in"] = pd.to_numeric(bus_vol["in"], errors="coerce")
    bus_vol["out"] = pd.to_numeric(bus_vol["out"], errors="coerce")
    bus_vol = bus_vol.dropna(subset=['in','out'])

    # Remove negative passenger counts
    bus_vol = bus_vol[(bus_vol['in'] >= 0) & (bus_vol['out'] >= 0)]

    # Remove duplicates
    bus_vol = bus_vol.drop_duplicates(subset=['stop_id','hour','day','month'])

    _write(bus_vol, "clean_bus_vol", engine)

    del bus_vol
    gc.collect()

    # bus_line
    bus_line = _read("SELECT * FROM raw_bus_line", engine)

    # Drop "unnamed:_0" column
    bus_line.drop(columns=['unnamed:_0'], errors='ignore', inplace=True)

    # Standardize stop_id to string format
    bus_line['stop_id'] = bus_line['stop_id'].astype(str)

    # Convert time columns to datetime format
    time_cols = ['wd_firstbus','wd_lastbus','sat_firstbus','sat_lastbus','sun_firstbus','sun_lastbus']
    for col in time_cols:
        bus_line[col] = pd.to_datetime(bus_line[col], format='%H%M', errors='coerce')

    bus_line["distance"] = pd.to_numeric(bus_line["distance"], errors="coerce")

    # Remove negative distances
    bus_line = bus_line[bus_line['distance'] >= 0]

    # Remove duplicates
    bus_line = bus_line.drop_duplicates(subset=['line','direction','sequence','stop_id'])

    _write(bus_line, "clean_bus_line", engine)

    engine.dispose()
    del bus_line
    gc.collect()

def clean_tourist_attractions(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_tourist_attractions...")
    engine = get_mysql_engine(mysql_conn_id)

    tourist_attractions = _read("SELECT * FROM raw_tourist_attractions", engine)

    # Clean up unnecessary columns
    COLS_TO_DROP = [
        "external_link",
        "meta_description",
        "opening_hours",
        "inc_crc",
        "fmel_upd_d",
        "url_path",
        "image_path",
        "image_alt_text",
        "photocredits",
        "longitude", 
        "address",
        "postalcode",
        "lastmodified",
    ]

    # Drop duplicates, missing values and reset index
    tourist_attractions = tourist_attractions.drop(columns=[c for c in COLS_TO_DROP if c in tourist_attractions.columns])
    tourist_attractions = tourist_attractions.drop_duplicates()
    tourist_attractions = tourist_attractions.dropna()
    tourist_attractions = tourist_attractions.reset_index(drop=True)

    # Convert latitude and longitude to wgs84
    tourist_attractions["lat"] = pd.to_numeric(tourist_attractions["latitude"], errors="coerce")
    tourist_attractions["lng"] = pd.to_numeric(tourist_attractions["longtitude"], errors="coerce")

    # Keep only rows with valid coordinates
    tourist_attractions = tourist_attractions.dropna(subset=["lat", "lng"])

    # Singapore rough bounds (filters obvious outliers)
    sg = (tourist_attractions["lat"].between(1.15, 1.50)) & (tourist_attractions["lng"].between(103.55, 104.20))
    tourist_attractions = tourist_attractions.loc[sg]

    tourist_attractions = tourist_attractions.drop(
        columns=[c for c in ("latitude", "longtitude") if c in tourist_attractions.columns],
        errors="ignore",
    )

    _write(tourist_attractions, "clean_tourist_attractions", engine)

    engine.dispose()
    del tourist_attractions
    gc.collect()

def clean_resale_flat_price(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_resale_flat_price...")
    engine = get_mysql_engine(mysql_conn_id)

    resale = _read("SELECT * FROM raw_resale_flat_price", engine)

    # Convert columns to the right datatype
    resale["month"] = pd.to_datetime(resale["month"], format="%Y-%m")
    resale["resale_price"] = pd.to_numeric(resale["resale_price"], errors="coerce")
    resale["floor_area_sqm"] = pd.to_numeric(resale["floor_area_sqm"], errors="coerce")
    resale["lease_commence_date"] = pd.to_numeric(resale["lease_commence_date"], errors="coerce")

    _write(resale, "clean_resale_flat_price", engine)

    engine.dispose()
    del resale
    gc.collect()