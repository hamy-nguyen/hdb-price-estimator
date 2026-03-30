from sqlalchemy import create_engine, Integer, Float, String, DateTime
import pandas as pd
from sklearn.neighbors import BallTree
import numpy as np
from pyproj import Transformer
import re
from . import data_watermarking as dw
from airflow.hooks.base import BaseHook
import logging
import gc

logger = logging.getLogger(__name__)

R_EARTH_M = 6_371_000

def get_mysql_engine(mysql_conn_id):
    conn = BaseHook.get_connection(mysql_conn_id)

    return create_engine(
        f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )

# CHECK: Shift to helpers
def dtypes_dict_generator(dtypes):
    dtype_dict = {}

    for col, dtype in dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            dtype_dict[col] = Integer()
        elif pd.api.types.is_float_dtype(dtype):
            dtype_dict[col] = Float()
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            dtype_dict[col] = DateTime()
        else:
            dtype_dict[col] = String(255)

    return dtype_dict

def joinable_resale_prices(mysql_conn_id):
    logger.info("Preparing joinable resale flat prices data...")

    engine_hdb = get_mysql_engine(mysql_conn_id)

    # CHECK: change to clean_resale_prices later
    str_sql = f'''
    SELECT * FROM raw_resale_flat_price
    '''

    resale = pd.read_sql(sql=str_sql, con=engine_hdb)

    resale['full_address'] = resale['block'] + ' ' + resale['street_name']

    resale = resale.rename(columns={
        'month': 'month_and_year'
    })

    # will be computed again after all transformations and joins are done, but drop here to save memory
    resale = resale.drop(columns=["_fp", "block", "street_name"])

    # month and year columns
    resale['month_and_year'] = pd.to_datetime(resale['month_and_year'], format='%Y-%m')
    resale['month'] = resale['month_and_year'].dt.month
    resale['year'] = resale['month_and_year'].dt.year

    # CHECK: prepare dtypes for ingestion (might be changed in clean_resale_prices already)
    resale['floor_area_sqm'] = pd.to_numeric(resale['floor_area_sqm'])
    resale['lease_commence_date'] = pd.to_numeric(resale['lease_commence_date'])
    resale['resale_price'] = pd.to_numeric(resale['resale_price'])

    resale.to_sql(
        'transform_resale_flat_price',
        con=engine_hdb,
        if_exists='replace',
        index=False,
        dtype=dtypes_dict_generator(resale.dtypes)
    )

    del resale
    gc.collect()

def join_hdb(mysql_conn_id):
    logger.info("Joining HDB data...")

    engine_hdb = get_mysql_engine(mysql_conn_id)

    # CHECK: change to clean_hdb later
    str_sql = f'''
    SELECT 
        blk_no, street, market_hawker, multistorey_carpark,
        pln_area_n, region_n, lat, lng, max_floor_lvl, total_dwelling_units,
        year_completed
    FROM raw_hdb 
    '''

    hdb = pd.read_sql(sql=str_sql, con=engine_hdb)

    hdb['full_address'] = hdb['blk_no'] + ' ' + hdb['street']

    # convert Y/N to 1/0 for market_hawker and multistorey_carpark
    hdb['has_market_hawker'] = hdb['market_hawker'].map({'Y': 1, 'N': 0})
    hdb['has_multistorey_carpark'] = hdb['multistorey_carpark'].map({'Y': 1, 'N': 0})

    hdb = hdb.rename(columns={
        'pln_area_n': 'planning_area',
        'region_n': 'region'
    })

    hdb_new_cols = [
        'lat', 'lng', 'planning_area', 'region', 
        'max_floor_lvl', 'total_dwelling_units', 'has_market_hawker', 
        'has_multistorey_carpark', 'year_completed'
    ]
    
    str_sql = f'''
    SELECT * FROM transform_resale_flat_price 
    '''

    resale = pd.read_sql(sql=str_sql, con=engine_hdb)

    resale = resale.merge(
        hdb[["full_address"] + hdb_new_cols], 
        on='full_address', 
        how='left'
    )

    # CHECK: prepare dtypes for ingestion (might be changed in clean_hdb already)
    resale['lat'] = pd.to_numeric(resale['lat'])
    resale['lng'] = pd.to_numeric(resale['lng'])
    resale['max_floor_lvl'] = pd.to_numeric(resale['max_floor_lvl'])
    resale['total_dwelling_units'] = pd.to_numeric(resale['total_dwelling_units'])
    resale['year_completed'] = pd.to_numeric(resale['year_completed'])
    
    resale.to_sql(
        'transform_resale_flat_price',
        con=engine_hdb,
        if_exists='replace',
        index=False,
        dtype=dtypes_dict_generator(resale.dtypes)
    )

    del resale, hdb
    gc.collect()

def join_mrt(mysql_conn_id):
    logger.info("Joining MRT data...")

    engine_hdb = get_mysql_engine(mysql_conn_id)

    # CHECK: change to clean_mrt later
    str_sql = f'''
    SELECT 
        lat, lng, name
    FROM raw_mrt
    '''

    mrt = pd.read_sql(sql=str_sql, con=engine_hdb)

    # CHECK: prepare mrt coordinates (might be changed in clean_mrt already)
    mrt_coords = mrt[["lat", "lng"]].dropna().copy()
    mrt_coords["lat"] = pd.to_numeric(mrt_coords["lat"])
    mrt_coords["lng"] = pd.to_numeric(mrt_coords["lng"])
    mrt_rad = np.radians(mrt_coords[["lat", "lng"]].to_numpy())

    mrt_tree = BallTree(mrt_rad, metric="haversine")

    str_sql = f'''
    SELECT * FROM transform_resale_flat_price 
    '''

    resale = pd.read_sql(sql=str_sql, con=engine_hdb)

    block_coords = (
        resale[["full_address", "lat", "lng"]]
        .drop_duplicates(subset=["full_address"])
        .copy()
    )

    block_rad = np.radians(block_coords[["lat", "lng"]].to_numpy())

    dist_rad, idx = mrt_tree.query(block_rad, k=1)

    block_coords["dist_to_nearest_mrt_m"] = np.asarray(dist_rad).ravel() * R_EARTH_M
    block_coords["nearest_mrt"] = mrt["name"].iloc[np.asarray(idx).ravel()].values
    block_coords["n_mrt_within_1km"] = mrt_tree.query_radius(block_rad, r=1000/R_EARTH_M, count_only=True)

    mrt_new_cols = ["dist_to_nearest_mrt_m", "nearest_mrt", "n_mrt_within_1km"]

    resale = resale.merge(
        block_coords[["full_address"] + mrt_new_cols],
        on="full_address",
        how="left"
    )
    
    resale.to_sql(
        'transform_resale_flat_price',
        con=engine_hdb,
        if_exists='replace',
        index=False,
        dtype=dtypes_dict_generator(resale.dtypes)
    )

    del resale, block_coords, mrt, mrt_coords, mrt_rad, mrt_tree
    gc.collect()

def join_poi(mysql_conn_id):
    logger.info("Joining POI data...")

    engine_hdb = get_mysql_engine(mysql_conn_id)

    # CHECK: change to clean_poi later
    str_sql = f'''
    SELECT 
        school, primary_school, secondary_school, shopping_mall,
        restaurant, food, cafe, meal_takeaway, park, supermarket,
        grocery_or_supermarket, lat, lng
    FROM raw_poi
    '''

    poi = pd.read_sql(sql=str_sql, con=engine_hdb)

    # Define POI category groups based on boolean columns in poi.csv
    poi_categories = {
        "school": ["school", "primary_school", "secondary_school"],
        "mall": ["shopping_mall"],
        "food": ["restaurant", "food", "cafe", "meal_takeaway"],
        "park": ["park"],
        "supermarket": ["supermarket", "grocery_or_supermarket"],
    }

    # CHECK: prepare poi coordinates (might be changed in clean_poi already)
    poi_coords = poi.dropna(subset=["lat", "lng"]).copy()
    poi_coords["lat"] = pd.to_numeric(poi_coords["lat"])
    poi_coords["lng"] = pd.to_numeric(poi_coords["lng"])
    poi_rad = np.radians(poi_coords[["lat", "lng"]].to_numpy())

    str_sql = f'''
    SELECT * FROM transform_resale_flat_price 
    '''

    resale = pd.read_sql(sql=str_sql, con=engine_hdb)

    block_coords = (
        resale[["full_address", "lat", "lng"]]
        .drop_duplicates(subset=["full_address"])
        .copy()
    )

    block_rad = np.radians(block_coords[["lat", "lng"]].to_numpy())

    poi_new_cols: list[str] = []

    for cat_name, cat_cols in poi_categories.items():
        # Filter POIs that match any column in the category
        valid_cols = [c for c in cat_cols if c in poi_coords.columns]

        if not valid_cols:
            continue

        mask = poi_coords[valid_cols].apply(
            lambda col: col.astype(str).str.upper() == "TRUE"
        ).any(axis=1)

        cat_pois = poi_coords.loc[mask, ["lat", "lng"]]

        if cat_pois.empty:
            continue

        cat_rad = np.radians(cat_pois[["lat", "lng"]].to_numpy())
        cat_tree = BallTree(cat_rad, metric="haversine")

        # Nearest distance
        dist, _ = cat_tree.query(block_rad, k=1)
        col_dist = f"dist_to_{cat_name}_m"
        block_coords[col_dist] = np.asarray(dist).ravel() * R_EARTH_M
        poi_new_cols.append(col_dist)

        # Count within 1 km
        radius_rad = 1000 / R_EARTH_M
        counts = cat_tree.query_radius(block_rad, r=radius_rad, count_only=True)
        col_count = f"n_{cat_name}_within_1km"
        block_coords[col_count] = np.asarray(counts)
        poi_new_cols.append(col_count)

    resale = resale.merge(
        block_coords[["full_address"] + poi_new_cols],
        on="full_address",
        how="left"
    )
    
    resale.to_sql(
        'transform_resale_flat_price',
        con=engine_hdb,
        if_exists='replace',
        index=False,
        dtype=dtypes_dict_generator(resale.dtypes)
    )

    del resale, block_coords, poi, poi_coords, poi_rad, cat_pois, cat_rad, cat_tree
    gc.collect()

def join_onemap(mysql_conn_id):
    logger.info("Joining OneMap data...")

    engine_hdb = get_mysql_engine(mysql_conn_id)

    # CHECK: change to clean_onemap later
    str_sql = f'''
    SELECT * FROM raw_onemap_transport_school
    '''

    transport_school = pd.read_sql(sql=str_sql, con=engine_hdb)

    str_sql = f'''
    SELECT * FROM transform_resale_flat_price 
    '''

    resale = pd.read_sql(sql=str_sql, con=engine_hdb)

    resale['onemap_join_year'] = resale['year'].apply(
        lambda x: 2015 if x < 2020 else 2020
    )
    
    transport_cols = [
        'bus', 'mrt', 'mrt_bus', 'mrt_car',
        'mrt_other', 'taxi', 'car', 'pvt_chartered_bus', 'lorry_pickup',
        'motorcycle_scooter', 'others', 'no_transport_required',
        'other_combi_mrt_or_bus', 'mrt_lrt_only', 'mrt_lrt_and_bus',
        'other_combi_mrt_lrt_or_bus', 'taxi_pvt_hire_car_only',
        'pvt_chartered_bus_van'
    ]

    transport_school[transport_cols] = (
        transport_school[transport_cols]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0)
    )

    transport_school['total_transport_school'] = transport_school[transport_cols].sum(axis=1)

    denom = transport_school['total_transport_school'].replace(0, np.nan)

    transport_school['transport_school_pct_bus'] = transport_school['bus'] / denom
    transport_school['transport_school_pct_mrt'] = transport_school['mrt'] / denom
    transport_school['transport_school_pct_mrt_bus'] = transport_school['mrt_bus'] / denom
    transport_school['transport_school_pct_car'] = transport_school['car'] / denom
    transport_school['transport_school_pct_mrt_lrt_only'] = transport_school['mrt_lrt_only'] / denom
    transport_school['transport_school_pct_mrt_lrt_and_bus'] = transport_school['mrt_lrt_and_bus'] / denom

    transport_school.fillna(0, inplace=True)

    transport_school['onemap_join_year'] = pd.to_numeric(transport_school['year'])

    transport_school_new_cols = [
        'transport_school_pct_bus', 'transport_school_pct_mrt', 'transport_school_pct_mrt_bus',
        'transport_school_pct_car', 'transport_school_pct_mrt_lrt_only', 'transport_school_pct_mrt_lrt_and_bus'
    ]

    resale = resale.merge(
        transport_school[["planning_area", "onemap_join_year"] + transport_school_new_cols],
        on=['planning_area', 'onemap_join_year'],
        how="left"
    )

    # CHECK: change to clean_onemap later
    str_sql = f'''
    SELECT * FROM raw_onemap_transport_work
    '''

    transport_work = pd.read_sql(sql=str_sql, con=engine_hdb)

    transport_work[transport_cols] = (
        transport_work[transport_cols]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0)
    )

    transport_work['total_transport_work'] = transport_work[transport_cols].sum(axis=1)

    denom = transport_work['total_transport_work'].replace(0, np.nan)

    transport_work['transport_work_pct_bus'] = transport_work['bus'] / denom
    transport_work['transport_work_pct_mrt'] = transport_work['mrt'] / denom
    transport_work['transport_work_pct_mrt_bus'] = transport_work['mrt_bus'] / denom
    transport_work['transport_work_pct_car'] = transport_work['car'] / denom
    transport_work['transport_work_pct_mrt_lrt_only'] = transport_work['mrt_lrt_only'] / denom
    transport_work['transport_work_pct_mrt_lrt_and_bus'] = transport_work['mrt_lrt_and_bus'] / denom

    transport_work.fillna(0, inplace=True)

    transport_work['onemap_join_year'] = pd.to_numeric(transport_work['year'])

    transport_work_new_cols = [
        'transport_work_pct_bus', 'transport_work_pct_mrt', 'transport_work_pct_mrt_bus',
        'transport_work_pct_car', 'transport_work_pct_mrt_lrt_only', 'transport_work_pct_mrt_lrt_and_bus'
    ]

    resale = resale.merge(
        transport_work[["planning_area", "onemap_join_year"] + transport_work_new_cols],
        on=['planning_area', 'onemap_join_year'],
        how="left"
    )

    # CHECK: change to clean_onemap later
    str_sql = f'''
    SELECT * FROM raw_onemap_tenancy
    '''

    tenancy_cols = ['owner', 'tenant', 'others']

    tenancy = pd.read_sql(sql=str_sql, con=engine_hdb)

    tenancy[tenancy_cols] = (
        tenancy[tenancy_cols]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0)
    )

    tenancy['total_tenancy'] = tenancy[tenancy_cols].sum(axis=1)

    denom = tenancy['total_tenancy'].replace(0, np.nan)

    tenancy['tenancy_pct_owner'] = tenancy['owner'] / denom
    tenancy['tenancy_pct_tenant'] = tenancy['tenant'] / denom

    tenancy.fillna(0, inplace=True)

    tenancy['onemap_join_year'] = pd.to_numeric(tenancy['year'])

    tenancy_new_cols = [
        'tenancy_pct_owner', 'tenancy_pct_tenant'
    ]

    resale = resale.merge(
        tenancy[["planning_area", "onemap_join_year"] + tenancy_new_cols],
        on=['planning_area', 'onemap_join_year'],
        how="left"
    )

    # CHECK: change to clean_onemap later
    str_sql = f'''
    SELECT * FROM raw_onemap_dwelling
    '''

    dwelling_cols = [
      'hdb_1_and_2_room_flats',
      'hdb_3_room_flats',
      'hdb_4_room_flats',
      'hdb_5_room_and_executive_flats',
      'condominiums_and_other_apartments',
      'landed_properties',
      'others'
    ]

    dwelling = pd.read_sql(sql=str_sql, con=engine_hdb)

    dwelling[dwelling_cols] = (
        dwelling[dwelling_cols]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0)
    )

    dwelling['total_dwelling'] = dwelling[dwelling_cols].sum(axis=1)

    denom = dwelling['total_dwelling'].replace(0, np.nan)

    dwelling['dwelling_pct_hdb_1_and_2_room_flats'] = dwelling['hdb_1_and_2_room_flats'] / denom
    dwelling['dwelling_pct_hdb_3_room_flats'] = dwelling['hdb_3_room_flats'] / denom
    dwelling['dwelling_pct_hdb_4_room_flats'] = dwelling['hdb_4_room_flats'] / denom
    dwelling['dwelling_pct_hdb_5_room_and_executive_flats'] = dwelling['hdb_5_room_and_executive_flats'] / denom
    dwelling['dwelling_pct_condominiums_and_other_apartments'] = dwelling['condominiums_and_other_apartments'] / denom
    dwelling['dwelling_pct_landed_properties'] = dwelling['landed_properties'] / denom

    dwelling.fillna(0, inplace=True)

    dwelling['onemap_join_year'] = pd.to_numeric(dwelling['year'])

    dwelling_new_cols = [
        'dwelling_pct_hdb_1_and_2_room_flats', 'dwelling_pct_hdb_3_room_flats', 'dwelling_pct_hdb_4_room_flats',
        'dwelling_pct_hdb_5_room_and_executive_flats', 'dwelling_pct_condominiums_and_other_apartments', 'dwelling_pct_landed_properties'
    ]

    resale = resale.merge(
        dwelling[["planning_area", "onemap_join_year"] + dwelling_new_cols],
        on=['planning_area', 'onemap_join_year'],
        how="left"
    )

    # drop onemap_join_year
    resale = resale.drop(columns=['onemap_join_year'])
    
    resale.to_sql(
        'transform_resale_flat_price',
        con=engine_hdb,
        if_exists='replace',
        index=False,
        dtype=dtypes_dict_generator(resale.dtypes)
    )

    del resale, transport_school, transport_work, tenancy, dwelling
    gc.collect()

def join_car_park(mysql_conn_id):
    logger.info("Joining car park data...")

    engine_hdb = get_mysql_engine(mysql_conn_id)

    # CHECK: change to clean_carpark later
    str_sql = f'''
    SELECT 
        x_coord, y_coord, car_park_no, free_parking, short_term_parking, 
        night_parking, gantry_height, car_park_basement, car_park_decks
    FROM raw_carpark
    '''

    car_park = pd.read_sql(sql=str_sql, con=engine_hdb)

    # CHECK: convert x and y coord to lat and lng (might be changed in clean_carpark already)
    transformer = Transformer.from_crs("EPSG:3414", "EPSG:4326", always_xy=True)

    car_park['lat'], car_park['lng'] = transformer.transform(
        car_park['x_coord'].values,
        car_park['y_coord'].values
    )

    car_park_coords = car_park[["lat", "lng"]].dropna().copy()
    car_park_coords["lat"] = pd.to_numeric(car_park_coords["lat"])
    car_park_coords["lng"] = pd.to_numeric(car_park_coords["lng"])
    carpark_rad = np.radians(car_park_coords[["lat", "lng"]].to_numpy())

    carpark_tree = BallTree(carpark_rad, metric="haversine")

    str_sql = f'''
    SELECT * FROM transform_resale_flat_price 
    '''

    resale = pd.read_sql(sql=str_sql, con=engine_hdb)

    block_coords = (
        resale[["full_address", "lat", "lng"]]
        .drop_duplicates(subset=["full_address"])
        .copy()
    )
    block_rad = np.radians(block_coords[["lat", "lng"]].to_numpy())

    dist_rad, idx = carpark_tree.query(block_rad, k=1)
    block_coords["dist_to_nearest_carpark_m"] = np.asarray(dist_rad).ravel() * R_EARTH_M
    block_coords["nearest_carpark"] = car_park["car_park_no"].iloc[np.asarray(idx).ravel()].values

    # Count within 500m
    carpark_radius_rad = 500 / R_EARTH_M
    carpark_counts = carpark_tree.query_radius(block_rad, r=carpark_radius_rad, count_only=True)
    block_coords["n_carparks_within_500m"] = np.asarray(carpark_counts)

    carpark_new_cols = ["dist_to_nearest_carpark_m", "nearest_carpark", "n_carparks_within_500m"]

    resale = resale.merge(
        block_coords[["full_address"] + carpark_new_cols],
        on="full_address",
        how="left"
    )

    # CHECK: if dtypes successful (preparation might be done in clean_carpark already)
    # convert YES/NO to 1/0
    car_park['has_free_parking'] = (car_park['free_parking'] != "NO").astype(int)
    car_park['is_free_daytime'] = car_park['free_parking'].str.contains("7AM-10.30PM", na=False).astype(int)
    car_park['is_free_halfday'] = car_park['free_parking'].str.contains("1PM-10.30PM", na=False).astype(int)
    car_park['has_short_term_parking'] = (car_park['short_term_parking'] != "NO").astype(int)
    car_park['has_night_parking'] =  car_park['night_parking'].map({'YES': 1, 'NO': 0})
    car_park['is_visitor_friendly'] = (
        (car_park['has_short_term_parking'] == 1) &
        (car_park['has_free_parking'] == 1) &
        (car_park['has_night_parking'] == 1)
    ).astype(int)

    car_park['gantry_height'] = pd.to_numeric(car_park['gantry_height'], errors='coerce').fillna(0)

    car_park['has_height_restriction'] = (car_park['gantry_height'] > 0).astype(int)
    car_park['has_big_vehicle_restriction'] = (
        (car_park['has_height_restriction'] == 1) &
        (car_park['gantry_height'] < 2.15)
    ).astype(int)
    car_park['has_car_park_basement'] = car_park['car_park_basement'].map({'Y': 1, 'N': 0})

    carpark_new_cols = [
        "gantry_height", "car_park_decks", "has_free_parking",
        "is_free_daytime", "is_free_halfday", "has_short_term_parking",
        "has_night_parking", "is_visitor_friendly", "has_height_restriction",
        "has_big_vehicle_restriction", "has_car_park_basement"
    ]

    resale = resale.merge(
        car_park[["car_park_no"] + carpark_new_cols],
        left_on="nearest_carpark",
        right_on="car_park_no",
        how="left"
    )

    resale = resale.drop(columns=["car_park_no"])

    # CHECK: prepare dtypes for ingestion (might be changed in clean_carpark already)
    resale['car_park_decks'] = pd.to_numeric(resale['car_park_decks'])
    
    resale.to_sql(
        'transform_resale_flat_price',
        con=engine_hdb,
        if_exists='replace',
        index=False,
        dtype=dtypes_dict_generator(resale.dtypes)
    )

    del resale, block_coords, car_park, car_park_coords, carpark_rad, carpark_tree
    gc.collect()

def join_bus(mysql_conn_id):
    logger.info("Joining bus data...")

    engine_hdb = get_mysql_engine(mysql_conn_id)

    # CHECK: change to clean_bus_line later
    str_sql = f'''
    SELECT 
        busstopcode, latitude, longitude
    FROM raw_bus_stops
    '''

    bus_stops = pd.read_sql(sql=str_sql, con=engine_hdb)

    # CHECK: prepare bus stop coordinates (might be changed in clean_bus_stops already)
    bus_stops.rename(columns={
        'busstopcode': 'stop_id',
        'latitude': 'lat',
        'longitude': 'lng'
    }, inplace=True)

    # CHECK: change to clean_bus_line later
    str_sql = f'''
    SELECT 
        stop_id, wd_firstbus, wd_lastbus, sat_firstbus,
        sat_lastbus, sun_firstbus, sun_lastbus
    FROM raw_bus_line
    '''

    bus_line = pd.read_sql(sql=str_sql, con=engine_hdb)

    # Stop-level operating days per week
    bus_operating = bus_line.groupby('stop_id').agg({
        'wd_firstbus': lambda x: x.notna().any(),
        'wd_lastbus':  lambda x: x.notna().any(),
        'sat_firstbus': lambda x: x.notna().any(),
        'sat_lastbus':  lambda x: x.notna().any(),
        'sun_firstbus': lambda x: x.notna().any(),
        'sun_lastbus':  lambda x: x.notna().any()
    }).reset_index()

    # Convert service availability into days/week
    bus_operating['operates_weekday'] = bus_operating[['wd_firstbus', 'wd_lastbus']].any(axis=1).astype(int)
    bus_operating['operates_sat'] = bus_operating[['sat_firstbus', 'sat_lastbus']].any(axis=1).astype(int)
    bus_operating['operates_sun'] = bus_operating[['sun_firstbus', 'sun_lastbus']].any(axis=1).astype(int)

    bus_operating['operating_days_per_week'] = (
        bus_operating['operates_weekday'] * 5
        + bus_operating['operates_sat'] * 1
        + bus_operating['operates_sun'] * 1
    )

    bus_operating = bus_operating[['stop_id', 'operating_days_per_week']]

    # CHECK: change to clean_bus_vol later
    str_sql = f'''
    SELECT 
        `in`, `out`, stop_id, hour
    FROM raw_bus_vol
    '''

    bus_vol = pd.read_sql(sql=str_sql, con=engine_hdb)

    # CHECK: Add total volume column (might have been done in clean_bus_vol already)
    bus_vol['in'] = pd.to_numeric(bus_vol['in'])
    bus_vol['out'] = pd.to_numeric(bus_vol['out'])
    bus_vol['total_volume'] = bus_vol['in'] + bus_vol['out']

    # 24 summary values per stop
    # Average passenger volume for each stop-hour pair
    bus_stop_hourly = (
        bus_vol.groupby(['stop_id', 'hour'], as_index=False)['total_volume']
        .mean()
        .rename(columns={'total_volume': 'avg_volume_at_hour'})
    )

    # One summary value per stop
    # average passenger volume per hour across all hours (Basically how busy is the bus stop per day per hour)
    bus_hourly_summary = (
        bus_stop_hourly.groupby('stop_id', as_index=False)['avg_volume_at_hour']
        .mean()
        .rename(columns={'avg_volume_at_hour': 'avg_passenger_volume_per_hour'})
    )

    # Merge stop-level features
    bus_stop_features = bus_stops.merge(
        bus_operating, 
        on='stop_id', 
        how='left'
    )

    bus_stop_features = bus_stop_features.merge(
        bus_hourly_summary,
        on='stop_id',
        how='left'
    )
    
    # CHECK: Prepare bus coordinates
    bus_coords = bus_stop_features[["lat", "lng"]].dropna().copy()
    bus_coords["lat"] = pd.to_numeric(bus_coords["lat"])
    bus_coords["lng"] = pd.to_numeric(bus_coords["lng"])
    bus_rad = np.radians(bus_coords[["lat", "lng"]].to_numpy())

    bus_tree = BallTree(bus_rad, metric="haversine")

    str_sql = f'''
    SELECT * FROM transform_resale_flat_price 
    '''

    resale = pd.read_sql(sql=str_sql, con=engine_hdb)

    # Compute distance for each unique (address) to avoid redundant queries
    block_coords = (
        resale[["full_address", "lat", "lng"]]
        .drop_duplicates(subset=["full_address"])
        .copy()
    )

    block_rad = np.radians(block_coords[["lat", "lng"]].to_numpy())

    dist_rad, idx = bus_tree.query(block_rad, k=1)
    
    block_coords["dist_to_nearest_bus_stop_m"] = np.asarray(dist_rad).ravel() * R_EARTH_M
    block_coords["nearest_bus_stop"] = bus_stop_features["stop_id"].iloc[np.asarray(idx).ravel()].values
    block_coords["n_bus_stop_within_1km"] = bus_tree.query_radius(block_rad, r=1000/R_EARTH_M, count_only=True)
    nearest_idx = idx.flatten()
    block_coords["nearest_bus_stop_operating_days_per_week"] = bus_stop_features.iloc[nearest_idx]['operating_days_per_week'].values
    block_coords["nearest_bus_stop_busyness_level"] = bus_stop_features.iloc[nearest_idx]['avg_passenger_volume_per_hour'].values

    bus_new_cols = [
        "dist_to_nearest_bus_stop_m",
        "nearest_bus_stop",
        "n_bus_stop_within_1km",
        "nearest_bus_stop_operating_days_per_week",
        "nearest_bus_stop_busyness_level"
    ]

    # Merge back to full df
    resale = resale.merge(
        block_coords[["full_address"] + bus_new_cols],
        on=["full_address"], 
        how="left"
    )
    
    resale.to_sql(
        'transform_resale_flat_price',
        con=engine_hdb,
        if_exists='replace',
        index=False,
        dtype=dtypes_dict_generator(resale.dtypes)
    )

    del resale, block_coords, bus_stops, bus_line, bus_vol, bus_operating, bus_stop_hourly, bus_hourly_summary, bus_stop_features, bus_coords, bus_rad, bus_tree
    gc.collect()

def join_tourist_attractions(mysql_conn_id):
    logger.info("Joining tourist attractions data...")

    engine_hdb = get_mysql_engine(mysql_conn_id)

    str_sql = f'''
    SELECT * FROM transform_resale_flat_price 
    '''

    resale = pd.read_sql(sql=str_sql, con=engine_hdb)

    del resale
    gc.collect()

def transform_resale_prices(mysql_conn_id):
    logger.info("Transforming resale flat prices data...")

    engine_hdb = get_mysql_engine(mysql_conn_id)

    str_sql = f'''
    SELECT * FROM transform_resale_flat_price 
    '''

    resale = pd.read_sql(sql=str_sql, con=engine_hdb)

    # rename lat/long
    resale = resale.rename(columns={
        'lat': 'latitude',
        'lng': 'longitude'
    })

    # CHECK: compute building_age
    resale['year'] = pd.to_numeric(resale['year'])
    resale['year_completed'] = pd.to_numeric(resale['year_completed'])
    resale['building_age'] = resale['year'] - resale['year_completed']

    # CHECK: Quarter and month index
    resale["month_and_year"] = pd.to_datetime(
        resale["month_and_year"], format="%Y-%m"
    )
    resale['quarter'] = resale['month_and_year'].dt.quarter

    min_month = resale["month_and_year"].min()
    resale["month_index"] = (
        (resale["month_and_year"].dt.year - min_month.year) * 12
        + (resale["month_and_year"].dt.month - min_month.month)
    )

    # CHECK: Rolling 6-month median price per town (lagged by 1 month to avoid leakage)
    resale['resale_price'] = pd.to_numeric(resale['resale_price'])

    resale = resale.sort_values(["town", "month_and_year"])
    town_monthly_median = (
        resale.groupby(["town", "month_and_year"])["resale_price"]
        .median()
        .reset_index()
        .rename(columns={"resale_price": "town_median_price"})
    )
    town_monthly_median = town_monthly_median.sort_values(["town", "month_and_year"])
    town_monthly_median["town_price_trend_6m"] = (
        town_monthly_median
        .groupby("town")["town_median_price"]
        .transform(lambda x: x.rolling(6, min_periods=1).mean().shift(1))
    )
    resale = resale.merge(
        town_monthly_median[["town", "month_and_year", "town_price_trend_6m"]],
        on=["town", "month_and_year"],
        how="left"
    )

    # Log-transformed target
    resale["log_resale_price"] = np.log1p(resale["resale_price"])

    # CHECK: Lease age (how old the lease is, not remaining)
    def parse_remaining_lease(s: str) -> float: # to remove if this is already computed in clean_resale_prices
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

    resale["remaining_lease_years"] = resale["remaining_lease"].apply(parse_remaining_lease)
    resale["lease_age"] = 99 - resale["remaining_lease_years"]
    resale["lease_age_sq"] = resale["lease_age"] ** 2

    # CHECK: price per sqm
    resale["floor_area_sqm"] = pd.to_numeric(resale["floor_area_sqm"])
    resale["price_per_sqm"] = resale["resale_price"] / resale["floor_area_sqm"]

    # CHECK: Interaction: floor area × storey midpoint
    storey_order = sorted(resale["storey_range"].unique(), key=lambda x: int(x.split(" TO ")[0]))
    resale["storey_range"] = pd.Categorical(resale["storey_range"], categories=storey_order, ordered=True)
    resale["storey_mid"] = resale["storey_range"].apply(
        lambda x: np.mean([int(v) for v in str(x).split(" TO ")])
    )
    resale["storey_mid"] = pd.to_numeric(resale["storey_mid"], errors='coerce').fillna(0)
    resale["floor_area_x_storey"] = resale["floor_area_sqm"] * resale["storey_mid"]

    # CHECK: Storey relative to building max — how high up within its building
    resale["max_floor_lvl"] = pd.to_numeric(resale["max_floor_lvl"], errors='coerce').fillna(0)
    resale["storey_ratio"] = resale["storey_mid"] / resale["max_floor_lvl"]

    # compute _fp column again (part by part to avoid memory issues)
    logging.info("Recomputing fingerprint column after all transformations and joins...")

    chunk_size = 20000
    first = True

    for start in range(0, len(resale), chunk_size):
        chunk = resale.iloc[start:start + chunk_size].copy()
        chunk = dw.add_fingerprint_column(chunk)

        chunk.to_sql(
            'transform_resale_flat_price',
            con=engine_hdb,
            if_exists="replace" if first else "append",
            index=False,
            dtype=dtypes_dict_generator(resale.dtypes),
            method="multi",
            chunksize=1000,
        )

        first = False
        del chunk
        gc.collect()
    
    del resale
    gc.collect()