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
from sqlalchemy import create_engine, text

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

def clean_tourist_attractions(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_tourist_attractions...")
    engine = get_mysql_engine(mysql_conn_id)

    df = _read("SELECT * FROM raw_tourist_attractions", engine)
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
	"longtitude", 
	"address",
	"postalcode",
	"lastmodified",
    ]
    # Drop duplicates, missing values and reset index
    df = df.drop(columns=[c for c in COLS_TO_DROP if c in df.columns])
    df = df.drop_duplicates()
    df = df.dropna()
    df = df.reset_index(drop=True)
    # Convert latitude and longitude to wgs84
    lon_col = "longitude" if "longitude" in df.columns else "longtitude"
    df["lat_wgs84"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["lon_wgs84"] = pd.to_numeric(df[lon_col], errors="coerce")
    # Keep only rows with valid coordinates
    df = df.dropna(subset=["lat_wgs84", "lon_wgs84"])
    # Singapore rough bounds (filters obvious outliers)
    sg = (df["lat_wgs84"].between(1.15, 1.50)) & (df["lon_wgs84"].between(103.55, 104.20))
    df = df.loc[sg]

    df = df.drop(
        columns=[c for c in ("latitude", "longitude", "longtitude") if c in df.columns],
        errors="ignore",
    )
    attr_df = df.copy()

    _write(df, "clean_tourist_attractions", engine)
    engine.dispose()
    del df
    gc.collect()


def clean_carpark(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_carpark...")
    engine = get_mysql_engine(mysql_conn_id)
    from pyproj import Transformer

    carpark_df = _read("SELECT * FROM raw_carpark", engine)
    # Convert columns to the right datatype
    carpark_df["x_coord"] = carpark_df["x_coord"].astype('float64')
    carpark_df['y_coord'] = carpark_df['y_coord'].astype('float64')
    carpark_df['car_park_decks'] = carpark_df['car_park_decks'].astype('int64')
    carpark_df['gantry_height'] = carpark_df['gantry_height'].astype('float64')
    # Drop duplicates in the dataset
    carpark_df = carpark_df.drop_duplicates()
    # Standardise categorical columns
    categorical_columns_for_standardisation = ["car_park_type", "type_of_parking_system", "short_term_parking", "free_parking"]
    for col in categorical_columns_for_standardisation:
        carpark_df[col] = carpark_df[col].str.strip().str.upper()
    # Convert boolean columns to int
    carpark_df["night_parking"] = carpark_df["night_parking"].map({"YES":1,"NO":0})
    carpark_df["car_park_basement"] = carpark_df["car_park_basement"].map({"Y":1,"N":0})
    # Drop rows with missing values
    carpark_df = carpark_df.dropna()
    # Reset index
    carpark_df = carpark_df.reset_index(drop=True)
    # Convert coordinates from SVY21 to WGS84
    transformer = Transformer.from_crs("EPSG:3414", "EPSG:4326")
    carpark_df["latitude"], carpark_df["longitude"] = transformer.transform(
        carpark_df["y_coord"].values,
        carpark_df["x_coord"].values
    )
    #Fix values that are outside of Singapore's Latitude and Longitude Range
    SG_LAT = (1.15, 1.47)
    SG_LNG = (103.6, 104.1)

    outlier_coords = (
        (carpark_df["latitude"] < SG_LAT[0]) | (carpark_df["latitude"] > SG_LAT[1]) |
        (carpark_df["longitude"] < SG_LNG[0]) | (carpark_df["longitude"] > SG_LNG[1])
    )
    carpark_df.loc[outlier_coords, ["latitude", "longitude"]] = None
    carpark_df = carpark_df.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
    #Handling Missing Values
    numeric_columns = ["car_park_decks", "gantry_height"]
    for numeric_col in numeric_columns:
        num_missing = carpark_df[numeric_col].isnull().sum()
        if (num_missing > 0):
            col_median = carpark_df[numeric_col].median()
            carpark_df[numeric_col] = carpark_df[numeric_col].fillna(col_median)
    # Handle missing categorical columns
    carpark_info_columns = ["car_park_type", "type_of_parking_system"]
    carpark_visitor_friendly_columns = ["short_term_parking", "free_parking"]
    for col in carpark_info_columns:
        num_missing = carpark_df[col].isnull().sum()
        if (num_missing > 0):
            col_mode = carpark_df[col].mode()[0]
            carpark_df[col] = carpark_df[col].fillna(col_mode)
    for col in carpark_visitor_friendly_columns:
        num_missing = carpark_df[col].isnull().sum()
        if (num_missing > 0):
            carpark_df[col] = carpark_df[col].fillna("NO")
    # Handle outliers
    carpark_numeric_columns = ["car_park_decks", "gantry_height"]
    for num_col in carpark_numeric_columns:
        q1 = carpark_df[num_col].quantile(0.25)
        q3 = carpark_df[num_col].quantile(0.75)
        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # Identify outliers
        outlier_mask = (
            (carpark_df[num_col] < lower_bound) |
            (carpark_df[num_col] > upper_bound)
        )
        if outlier_mask.sum() > 0:
            # Median (exclude surface car parks for cleaner median)
            median_val = carpark_df.loc[
                carpark_df["car_park_type"] != "SURFACE CAR PARK", num_col
            ].median()

            # Surface car parks: set to 0
            surface_mask = (
                outlier_mask &
                (carpark_df["car_park_type"] == "SURFACE CAR PARK")
            )

            carpark_df.loc[surface_mask, num_col] = 0

            # Non-surface: set to median
            nonsurface_mask = (
                outlier_mask &
                (carpark_df["car_park_type"] != "SURFACE CAR PARK")
            )

            carpark_df.loc[nonsurface_mask, num_col] = median_val
    _write(carpark_df, "clean_carpark", engine)
    engine.dispose()
    del carpark_df
    gc.collect()


def clean_resale_flat_price(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_resale_flat_price...")
    engine = get_mysql_engine(mysql_conn_id)
    import re
    import numpy as np

    df = _read("SELECT * FROM raw_resale_flat_price", engine)
    # Convert columns to the right datatype
    df["month"] = pd.to_datetime(df["month"], format="%Y-%m")
    df["year"] = df["month"].dt.year
    df["resale_price"] = pd.to_numeric(df["resale_price"], errors="coerce")
    df["floor_area_sqm"] = pd.to_numeric(df["floor_area_sqm"], errors="coerce")
    df["lease_commence_date"] = pd.to_numeric(df["lease_commence_date"], errors="coerce")
    # Parse remaining_lease string
    def parse_remaining_lease(s: str) -> float:
        """'61 years 04 months' → 61.33, '62 years' → 62.0"""
        if pd.isna(s) or str(s).strip() == "":
            return np.nan
        s = str(s).lower().strip()
        years, months = 0.0, 0.0
        y = re.search(r"(\d+)\s*year", s)
        m = re.search(r"(\d+)\s*month", s)
        if y:
            years = float(y.group(1))
        if m:
            months = float(m.group(1))
        return round(years + months / 12, 2)
    df["remaining_lease_years"] = df["remaining_lease"].apply(parse_remaining_lease)
    df["price_per_sqm"] = df["resale_price"] / df["floor_area_sqm"]
    # Deriving the price_per_sqm
    storey_order = sorted(df["storey_range"].unique(), key=lambda x: int(x.split(" TO ")[0]))
    df["storey_range"] = pd.Categorical(df["storey_range"], categories=storey_order, ordered=True)
    df["storey_mid"] = df["storey_range"].apply(
        lambda x: np.mean([int(v) for v in str(x).split(" TO ")])
    )

    df[["storey_range", "storey_mid"]].drop_duplicates().sort_values(by=["storey_mid"])  # type: ignore[call-overload]
    _write(df, "clean_resale_flat_price", engine)
    engine.dispose()
    del df
    gc.collect()


def clean_hdb(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_hdb...")
    engine = get_mysql_engine(mysql_conn_id)

    df = _read("SELECT * FROM raw_hdb", engine)
    hdb = df.copy()
    hdb.drop(columns=['Unnamed: 0'], errors='ignore', inplace=True)

    # Standardize text
    hdb['street'] = hdb['street'].str.upper()
    hdb['building'] = hdb['building'].str.upper()

    # Remove missing coordinates
    hdb = hdb.dropna(subset=['lat','lng'])
    hdb["total_dwelling_units"] = pd.to_numeric(
        hdb["total_dwelling_units"], errors="coerce"
    )
    # Remove negative dwelling values
    hdb = hdb[hdb['total_dwelling_units'] > 0]

    # Remove duplicates
    hdb = hdb.drop_duplicates(subset=['blk_no','street'])
    df = hdb.copy()
    _write(df, "clean_hdb", engine)
    engine.dispose()
    del df
    gc.collect()


def clean_poi(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_poi...")
    engine = get_mysql_engine(mysql_conn_id)

    df = _read("SELECT * FROM raw_poi", engine)

    poi = df.copy()
    # Drop "Unnamed: 0" column
    poi.drop(columns=['Unnamed: 0'], errors='ignore', inplace=True)

    # Dropping other not so helpful columns
    poi.drop(columns=['price_level', 'brand', 'formatted_address', 'global_code'], inplace=True)

    # Drop rows with small missing location values (Just 2 rows)
    poi = poi.dropna(subset=[ 'pln_area_c', 'subzone_no', 'subzone_n',
        'subzone_c', 'pln_area_n', 'planning_area'
    ])

    # Remove duplicates
    poi = poi.drop_duplicates(subset=["place_id"])

    # MySQL / read_sql often returns numbers as strings → comparisons like >= 0 raise TypeError
    for col in ("lat", "lng", "rating", "user_ratings_total"):
        if col in poi.columns:
            poi[col] = pd.to_numeric(poi[col], errors="coerce")

    poi = poi.dropna(subset=["lat", "lng", "rating", "user_ratings_total"])

    poi = poi[poi["rating"].between(0, 5)]
    poi = poi[poi["user_ratings_total"] >= 0]

    # Define category groups 
    category_groups = {

        'food_beverage': [
            'food', 'restaurant', 'cafe', 'bakery', 'meal_takeaway',
            'meal_delivery', 'bar', 'liquor_store'
        ],

        'healthcare': [
            'health', 'hospital', 'doctor', 'dentist', 'pharmacy',
            'physiotherapist', 'drugstore', 'veterinary_care'
        ],

        'education': [
            'school', 'primary_school', 'secondary_school',
            'university', 'library'
        ],

        'retail_commerce': [
            'store', 'shopping_mall', 'department_store', 'clothing_store',
            'shoe_store', 'jewelry_store', 'home_goods_store',
            'furniture_store', 'electronics_store', 'book_store',
            'supermarket', 'grocery_or_supermarket', 'convenience_store',
            'pet_store', 'florist', 'beauty_salon', 'hair_care',
            'spa', 'laundry', 'bicycle_store', 'hardware_store'
        ],

        'transport': [
            'transit_station', 'bus_station', 'subway_station',
            'train_station', 'light_rail_station', 'taxi_stand',
            'airport', 'parking', 'gas_station', 'car_rental',
            'car_repair', 'car_wash', 'car_dealer'
        ],

        'finance_business': [
            'finance', 'bank', 'atm', 'insurance_agency',
            'accounting', 'real_estate_agency', 'lawyer',
            'travel_agency'
        ],

        'religious_civic': [
            'place_of_worship', 'church', 'mosque', 'hindu_temple',
            'synagogue', 'embassy', 'local_government_office',
            'city_hall', 'courthouse', 'police', 'fire_station',
            'post_office', 'funeral_home', 'cemetery'
        ],

        'recreation_entertainment': [
            'tourist_attraction', 'park', 'gym', 'museum',
            'movie_theater', 'night_club', 'amusement_park',
            'bowling_alley', 'aquarium', 'zoo', 'stadium',
            'art_gallery', 'campground', 'casino', 'natural_feature'
        ],

        'lodging_residential': [
            'lodging'
        ],

        'services_contractors': [
            'general_contractor', 'electrician', 'plumber',
            'locksmith', 'roofing_contractor', 'moving_company',
            'storage', 'painter'
        ],

        'public_place_general': [
            'establishment', 'point_of_interest', 'premise', 'subpremise'
        ]
    }

    # Create grouped category column
    for group, cols in category_groups.items():
        existing_cols = [c for c in cols if c in poi.columns]
        poi[group] = poi[existing_cols].any(axis=1)

    # Combined label
    group_cols = list(category_groups.keys())

    poi['poi_group'] = poi[group_cols].apply(
        lambda row: ','.join(row.index[row].tolist()) if row.any() else 'other',
        axis=1
    )
    df = poi.copy()
    _write(df, "clean_poi", engine)
    engine.dispose()
    del df
    gc.collect()



def clean_bus_vol(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_bus_vol...")
    engine = get_mysql_engine(mysql_conn_id)

    df = _read("SELECT * FROM raw_bus_vol", engine)

    bus_vol = df.copy()

    # Drop "Unnamed: 0" column
    bus_vol.drop(columns=['Unnamed: 0'], errors='ignore', inplace=True)

    # Standardize stop_id to string format
    bus_vol['stop_id'] = bus_vol['stop_id'].astype(str)
    # data type conversion
    bus_vol["in"] = pd.to_numeric(bus_vol["in"], errors="coerce")
    bus_vol["out"] = pd.to_numeric(bus_vol["out"], errors="coerce")

    # Remove negative passenger counts
    bus_vol = bus_vol[(bus_vol['in'] >= 0) & (bus_vol['out'] >= 0)]

    # Add total volume column
    bus_vol['total_volume'] = bus_vol['in'] + bus_vol['out']

    # Remove duplicates
    bus_vol = bus_vol.drop_duplicates(subset=['stop_id','hour','day','month'])

    # Adding coordinates information
    bus_stops = _read("SELECT * FROM raw_bus_stops", engine)
    bus_stops['busstopcode'] = bus_stops['busstopcode'].astype(str)
    bus_vol['stop_id'] = bus_vol['stop_id'].astype(str)

    bus_stops.rename(columns={
        'busstopcode': 'stop_id',
        'latitude': 'lat',
        'longitude': 'lng'
    }, inplace=True)

    bus_vol = bus_vol.merge(
        bus_stops[['stop_id', 'lat', 'lng']],
        on='stop_id',
        how='left'
    )

    # Remove missing coordinates
    bus_vol = bus_vol.dropna(subset=['lat','lng'])
    

    _write(df, "clean_bus_vol", engine)
    engine.dispose()
    del df
    gc.collect()


def clean_bus_line(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_bus_line...")
    engine = get_mysql_engine(mysql_conn_id)

    df = _read("SELECT * FROM raw_bus_line", engine)

    # Cleaning of Bus Line Dataset
    bus_line = df.copy()

    # Drop "Unnamed: 0" column
    bus_line.drop(columns=['Unnamed: 0'], errors='ignore', inplace=True)

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

    # Adding coordinates information
    bus_stops = _read("SELECT * FROM raw_bus_stops", engine)
    bus_stops['busstopcode'] = bus_stops['busstopcode'].astype(str)
    bus_line['stop_id'] = bus_line['stop_id'].astype(str)

    bus_stops.rename(columns={
        'busstopcode': 'stop_id',
        'latitude': 'lat',
        'longitude': 'lng'
    }, inplace=True)

    bus_line = bus_line.merge(
        bus_stops[['stop_id', 'lat', 'lng']],
        on='stop_id',
        how='left'
    )

    # Remove missing coordinates
    bus_line = bus_line.dropna(subset=['lat','lng'])
    df = bus_line.copy()

    _write(df, "clean_bus_line", engine)
    engine.dispose()
    del df
    gc.collect()


def clean_mrt(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_mrt...")
    engine = get_mysql_engine(mysql_conn_id)

    df = _read("SELECT * FROM raw_mrt", engine)

    mrt = df.copy()

    # Drop "Unnamed: 0" column
    mrt.drop(columns=['Unnamed: 0'], errors='ignore', inplace=True)

    # Standardize text
    mrt['name'] = mrt['name'].str.upper()

    # Remove missing coordinates
    mrt = mrt.dropna(subset=['lat','lng'])

    # Remove duplicates
    mrt = mrt.drop_duplicates(subset=['stop_id'])

    df = mrt.copy()
    _write(df, "clean_mrt", engine)
    engine.dispose()
    del df
    gc.collect()


def clean_onemap_transport_school(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_onemap_transport_school...")
    engine = get_mysql_engine(mysql_conn_id)

    df = _read("SELECT * FROM raw_onemap_transport_school", engine)

    df_transport_school = df.copy()
    df_transport_school = df_transport_school.fillna(0)
    transport_school_cols = [
        'bus', 'mrt', 'mrt_bus', 'mrt_car',
        'mrt_other', 'taxi', 'car', 'pvt_chartered_bus', 'lorry_pickup',
        'motorcycle_scooter', 'others', 'no_transport_required',
        'other_combi_mrt_or_bus', 'mrt_lrt_only', 'mrt_lrt_and_bus',
        'other_combi_mrt_lrt_or_bus', 'taxi_pvt_hire_car_only',
        'pvt_chartered_bus_van'
    ]

    df_transport_school[transport_school_cols] = df_transport_school[transport_school_cols].apply(
        pd.to_numeric, errors='coerce'
    )
    df = df_transport_school.copy()
    _write(df, "clean_onemap_transport_school", engine)
    engine.dispose()
    del df
    gc.collect()


def clean_onemap_transport_work(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_onemap_transport_work...")
    engine = get_mysql_engine(mysql_conn_id)

    df = _read("SELECT * FROM raw_onemap_transport_work", engine)

    transport_cols = [
    'bus', 'mrt', 'mrt_bus', 'mrt_car',
    'mrt_other', 'taxi', 'car', 'pvt_chartered_bus', 'lorry_pickup',
    'motorcycle_scooter', 'others', 'no_transport_required',
    'other_combi_mrt_or_bus', 'mrt_lrt_only', 'mrt_lrt_and_bus',
    'other_combi_mrt_lrt_or_bus', 'taxi_pvt_hire_car_only',
    'pvt_chartered_bus_van'
    ]

    df[transport_cols] = df[transport_cols].apply(pd.to_numeric, errors='coerce')

    _write(df, "clean_onemap_transport_work", engine)
    engine.dispose()
    del df
    gc.collect()


def clean_onemap_tenancy(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_onemap_tenancy...")
    engine = get_mysql_engine(mysql_conn_id)

    df = _read("SELECT * FROM raw_onemap_tenancy", engine)

    tenant_cols = ['owner', 'tenant', 'others']
    df[tenant_cols] = df[tenant_cols].apply(pd.to_numeric, errors='coerce')

    _write(df, "clean_onemap_tenancy", engine)
    engine.dispose()
    del df
    gc.collect()


def clean_onemap_dwelling(mysql_conn_id: str) -> None:
    logger.info("Cleaning raw_onemap_dwelling...")
    engine = get_mysql_engine(mysql_conn_id)

    df = _read("SELECT * FROM raw_onemap_dwelling", engine)

    df_dwelling = df.copy()
    df_dwelling = df_dwelling.fillna(0)

    dwelling_cols = [
        'hdb_1_and_2_room_flats',
        'hdb_3_room_flats',
        'hdb_4_room_flats',
        'hdb_5_room_and_executive_flats',
        'condominiums_and_other_apartments',
        'landed_properties',
        'others'
    ]

    df_dwelling[dwelling_cols] = df_dwelling[dwelling_cols].apply(
        pd.to_numeric, errors='coerce'
    )
    df = df_dwelling.copy()

    _write(df, "clean_onemap_dwelling", engine)
    engine.dispose()
    del df
    gc.collect()
