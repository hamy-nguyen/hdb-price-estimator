import os
import time
from pathlib import Path
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Repo root (parent of onemap/)
_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env")

# Set-up authentication for OneMap API
AUTH_URL = "https://www.onemap.gov.sg/api/auth/post/getToken"
PAYLOAD = {
    "email": os.environ["ONEMAP_EMAIL"],
    "password": os.environ["ONEMAP_EMAIL_PASSWORD"],
}
AUTH_RESPONSE = requests.post(AUTH_URL, json=PAYLOAD, timeout=15)

if not AUTH_RESPONSE.ok:
    print("Auth failed:", AUTH_RESPONSE.status_code)
    print(AUTH_RESPONSE.text)
    raise SystemExit

AUTH_DATA = AUTH_RESPONSE.json()
ACCESS_TOKEN = AUTH_DATA.get("access_token")
HEADERS = {"Authorization": ACCESS_TOKEN}

# AWS RDS MySQL credentials
host = os.environ["AWS_RDS_HOST"]
port = os.environ["AWS_RDS_PORT"]
user = os.environ["AWS_RDS_USER"]
password = os.environ["AWS_RDS_PASSWORD"]
db = "hdb-price-estimator"

# API endpoints
PLANNING_AREA_URL = "https://www.onemap.gov.sg/api/public/popapi/getPlanningareaNames"
TRANSPORT_TO_SCHOOL_URL = "https://www.onemap.gov.sg/api/public/popapi/getModeOfTransportSchool"
TRANSPORT_TO_WORK_URL = "https://www.onemap.gov.sg/api/public/popapi/getModeOfTransportWork"
TENANCY_URL = "https://www.onemap.gov.sg/api/public/popapi/getTenancy"
DWELLING_URL = "https://www.onemap.gov.sg/api/public/popapi/getTypeOfDwellingHousehold"

# Constants for population data
YEARS_POP_QUERY = [2000, 2010, 2015, 2020]
YEAR_MAP = {
    1998: 2000,
    2008: 2010,
    2014: 2015,
    2019: 2020
}

def ingest_planning_areas():
    '''
    Fetches planning areas over the years from the OneMap API and ingests it into the MySQL database.
    '''
    engine = create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    )

    years = [1998, 2008, 2014, 2019]

    all_planning_areas = []

    for year in years:
        params = {"year": year}
        response = requests.get(PLANNING_AREA_URL, params=params, headers=HEADERS, timeout=15)
        data = response.json()

        for planning_area in data['SearchResults']:
            all_planning_areas.append({
                "planning_area": planning_area['pln_area_n'],
                "year": year
            })
        
    planning_areas_df = pd.DataFrame(all_planning_areas)
    planning_areas_df.to_sql(
        "raw_onemap_planning_areas",
        con=engine,
        if_exists="replace",
        index=False
    )

    return planning_areas_df

def ingest_transport_to_school(planning_areas_df):
    '''
    Fetches transport to school data from the OneMap API for each planning area
    and ingests it into the MySQL database.
    '''
    engine = create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    )

    all_transport_school_data = []

    for _, row in planning_areas_df.iterrows():

        planning_area = row['planning_area']
        year = YEAR_MAP.get(row['year'], row['year'])

        print(f"Fetching school transport data for {planning_area} in {year}...")
        
        params = {
            "planningArea": planning_area,
            "year": year
        }

        while True:
            try:
                response = requests.get(
                    TRANSPORT_TO_SCHOOL_URL, 
                    params=params, 
                    headers=HEADERS
                )
            except:
                return None
            
            # rate limit exceeded
            if response.status_code == 429:
                print("Rate limit exceeded. Waiting before retrying...")
                time.sleep(10)  # wait for 10 seconds before retrying
                continue

            data = response.json()

            all_transport_school_data.append({
                "planning_area": planning_area,
                "year": year,
                "bus": data[0]['bus'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt": data[0]['mrt'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt_bus": data[0]['mrt_bus'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt_car": data[0]['mrt_car'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt_other": data[0]['mrt_other'] if isinstance(data, list) and len(data) > 0 else None,
                "taxi": data[0]['taxi'] if isinstance(data, list) and len(data) > 0 else None,
                "car": data[0]['car'] if isinstance(data, list) and len(data) > 0 else None,
                "pvt_chartered_bus": data[0]['pvt_chartered_bus'] if isinstance(data, list) and len(data) > 0 else None,
                "lorry_pickup": data[0]['lorry_pickup'] if isinstance(data, list) and len(data) > 0 else None,
                "motorcycle_scooter": data[0]['motorcycle_scooter'] if isinstance(data, list) and len(data) > 0 else None,
                "others": data[0]['others'] if isinstance(data, list) and len(data) > 0 else None,
                "no_transport_required": data[0]['no_transport_required'] if isinstance(data, list) and len(data) > 0 else None,
                "other_combi_mrt_or_bus": data[0]['other_combi_mrt_or_bus'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt_lrt_only": data[0]['mrt_lrt_only'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt_lrt_and_bus": data[0]['mrt_lrt_and_bus'] if isinstance(data, list) and len(data) > 0 else None,
                "other_combi_mrt_lrt_or_bus": data[0]['other_combi_mrt_lrt_or_bus'] if isinstance(data, list) and len(data) > 0 else None,
                "taxi_pvt_hire_car_only": data[0]['taxi_pvt_hire_car_only'] if isinstance(data, list) and len(data) > 0 else None,
                "pvt_chartered_bus_van": data[0]['pvt_chartered_bus_van'] if isinstance(data, list) and len(data) > 0 else None,
            })

            if isinstance(data, dict) and data.get("Result") == "No Data Available!":
                print(f"No data for {planning_area} in {year}")
                break

            break

    transport_school_df = pd.DataFrame(all_transport_school_data)
    transport_school_df.to_sql(
        "raw_onemap_transport_school",
        con=engine,
        if_exists="replace",
        index=False
    )

    return transport_school_df

def get_transport_to_work(planning_areas_df):
    '''
    Fetches transport to work data from the OneMap API for each planning area
    and saves it to a CSV file.
    '''
    all_transport_work_data = []

    for _, row in planning_areas_df.iterrows():

        planning_area = row['planning_area']
        year = YEAR_MAP.get(row['year'], row['year'])

        print(f"Fetching work transport data for {planning_area} in {year}...")
        
        params = {
            "planningArea": planning_area,
            "year": year
        }

        while True:
            try:
                response = requests.get(
                    TRANSPORT_TO_WORK_URL, 
                    params=params, 
                    headers=HEADERS
                )
            except:
                return None
            
            # rate limit exceeded
            if response.status_code == 429:
                print("Rate limit exceeded. Waiting before retrying...")
                time.sleep(10)  # wait for 10 seconds before retrying
                continue

            data = response.json()

            all_transport_work_data.append({
                "planning_area": planning_area,
                "year": year,
                "bus": data[0]['bus'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt": data[0]['mrt'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt_bus": data[0]['mrt_bus'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt_car": data[0]['mrt_car'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt_other": data[0]['mrt_other'] if isinstance(data, list) and len(data) > 0 else None,
                "taxi": data[0]['taxi'] if isinstance(data, list) and len(data) > 0 else None,
                "car": data[0]['car'] if isinstance(data, list) and len(data) > 0 else None,
                "pvt_chartered_bus": data[0]['pvt_chartered_bus'] if isinstance(data, list) and len(data) > 0 else None,
                "lorry_pickup": data[0]['lorry_pickup'] if isinstance(data, list) and len(data) > 0 else None,
                "motorcycle_scooter": data[0]['motorcycle_scooter'] if isinstance(data, list) and len(data) > 0 else None,
                "others": data[0]['others'] if isinstance(data, list) and len(data) > 0 else None,
                "no_transport_required": data[0]['no_transport_required'] if isinstance(data, list) and len(data) > 0 else None,
                "other_combi_mrt_or_bus": data[0]['other_combi_mrt_or_bus'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt_lrt_only": data[0]['mrt_lrt_only'] if isinstance(data, list) and len(data) > 0 else None,
                "mrt_lrt_and_bus": data[0]['mrt_lrt_and_bus'] if isinstance(data, list) and len(data) > 0 else None,
                "other_combi_mrt_lrt_or_bus": data[0]['other_combi_mrt_lrt_or_bus'] if isinstance(data, list) and len(data) > 0 else None,
                "taxi_pvt_hire_car_only": data[0]['taxi_pvt_hire_car_only'] if isinstance(data, list) and len(data) > 0 else None,
                "pvt_chartered_bus_van": data[0]['pvt_chartered_bus_van'] if isinstance(data, list) and len(data) > 0 else None,
            })

            if isinstance(data, dict) and data.get("Result") == "No Data Available!":
                print(f"No data for {planning_area} in {year}")
                break

            break

    transport_work_df = pd.DataFrame(all_transport_work_data)
    transport_work_df.to_csv(f"{_ROOT}/dataset/onemap_transport_to_work.csv", index=False)

    return transport_work_df

def get_tenancy(planning_areas_df):
    '''
    Fetches tenancy data from the OneMap API for each planning area
    and saves it to a CSV file.
    '''
    all_tenancy_data = []

    for _, row in planning_areas_df.iterrows():

        planning_area = row['planning_area']
        year = YEAR_MAP.get(row['year'], row['year'])

        print(f"Fetching tenancy data for {planning_area} in {year}...")
        
        params = {
            "planningArea": planning_area,
            "year": year
        }

        while True:
            try:
                response = requests.get(
                    TENANCY_URL, 
                    params=params, 
                    headers=HEADERS
                )
            except:
                return None
            
            # rate limit exceeded
            if response.status_code == 429:
                print("Rate limit exceeded. Waiting before retrying...")
                time.sleep(10)  # wait for 10 seconds before retrying
                continue

            data = response.json()

            all_tenancy_data.append({
                "planning_area": planning_area,
                "year": year,
                "owner": data[0]['owner'] if isinstance(data, list) and len(data) > 0 else None,
                "tenant": data[0]['tenant'] if isinstance(data, list) and len(data) > 0 else None,
                "others": data[0]['others'] if isinstance(data, list) and len(data) > 0 else None
            })

            if isinstance(data, dict) and data.get("Result") == "No Data Available!":
                print(f"No data for {planning_area} in {year}")
                break

            break

    tenancy_df = pd.DataFrame(all_tenancy_data)
    tenancy_df.to_csv(f"{_ROOT}/dataset/onemap_tenancy.csv", index=False)

    return tenancy_df

def get_dwelling_household(planning_areas_df):
    '''
    Fetches dwelling types (household-level) data from the OneMap API for each planning area
    and saves it to a CSV file.
    '''
    all_dwelling_data = []

    for _, row in planning_areas_df.iterrows():

        planning_area = row['planning_area']
        year = YEAR_MAP.get(row['year'], row['year'])

        print(f"Fetching dwelling data for {planning_area} in {year}...")
        
        params = {
            "planningArea": planning_area,
            "year": year
        }

        while True:
            try:
                response = requests.get(
                    DWELLING_URL, 
                    params=params, 
                    headers=HEADERS
                )
            except:
                return None
            
            # rate limit exceeded
            if response.status_code == 429:
                print("Rate limit exceeded. Waiting before retrying...")
                time.sleep(10)  # wait for 10 seconds before retrying
                continue

            data = response.json()

            all_dwelling_data.append({
                "planning_area": planning_area,
                "year": year,
                "hdb_1_and_2_room_flats": data[0]['hdb_1_and_2_room_flats'] if isinstance(data, list) and len(data) > 0 else None,
                "hdb_3_room_flats": data[0]['hdb_3_room_flats'] if isinstance(data, list) and len(data) > 0 else None,
                "hdb_4_room_flats": data[0]['hdb_4_room_flats'] if isinstance(data, list) and len(data) > 0 else None,
                "hdb_5_room_and_executive_flats": data[0]['hdb_5_room_and_executive_flats'] if isinstance(data, list) and len(data) > 0 else None,
                "condominiums_and_other_apartments": data[0]['condominiums_and_other_apartments'] if isinstance(data, list) and len(data) > 0 else None,
                "landed_properties": data[0]['landed_properties'] if isinstance(data, list) and len(data) > 0 else None,
                "others": data[0]['others'] if isinstance(data, list) and len(data) > 0 else None
            })

            if isinstance(data, dict) and data.get("Result") == "No Data Available!":
                print(f"No data for {planning_area} in {year}")
                break

            break

    dwelling_df = pd.DataFrame(all_dwelling_data)
    dwelling_df.to_csv(f"{_ROOT}/dataset/onemap_dwelling.csv", index=False)

    return dwelling_df

if __name__ == "__main__":
    engine = create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    )
    planning_areas_df = pd.read_sql(
        "SELECT * FROM raw_onemap_planning_areas",
        con=engine
    )
    ingest_transport_to_school(planning_areas_df)