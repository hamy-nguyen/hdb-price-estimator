from pathlib import Path
from dotenv import load_dotenv
import os
import requests
import pymysql
import pandas as pd
import time

# Repo root (parent of onemap/)
_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env")

SEARCH_URL = "https://www.onemap.gov.sg/api/common/elastic/search"

def onemap_authenticate():
    '''
    Authenticates with the OneMap API and returns the HEADERS containing the access token for subsequent API calls.
    '''
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
    
    return HEADERS # use headers=HEADERS in subsequent API calls

def search_resale_coordinates(HEADERS):
    '''
    Fetches coordinates and postal codes for each block and street name in the resale flat prices dataset.
    '''
    # currently hardcoded
    db = pymysql.connect(
        host="localhost",
        port=3306,
        user="airflow_user",
        password="password",
        database="HDB_Data",
    )

    with db.cursor() as cursor:
        cursor.execute("SELECT * FROM raw_resale_flat_prices")
        df_resale_prices = cursor.fetchall()

    db.commit()
    db.close()

    df_resale_prices = pd.DataFrame(
        df_resale_prices,
        columns=[
            "month",
            "town",
            "flat_type",
            "block",
            "street_name",
            "storey_range",
            "floor_area_sqm",
            "flat_model",
            "lease_commence_date",
            "remaining_lease",
            "resale_price"
        ])

    # prerequisite transformation
    df_resale_prices['block_and_street_name'] = df_resale_prices['block'] + ' ' + df_resale_prices['street_name']

    for index, row in df_resale_prices.iterrows():
        params = {
            "searchVal": row["block_and_street_name"],
            "returnGeom": "Y",
            "getAddrDetails": "Y",
            "pageNum": 1,
        }

        while True:
            try:
                print(f"Searching for coordinates of {row['block_and_street_name']}...")

                response = requests.get(
                    SEARCH_URL, 
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
            results = data.get("results", [])

            if not results:
                    print(f"No results for {row['block_and_street_name']}")
            else:
                res = results[0]

                df_resale_prices.at[index, "lat"] = res.get("LATITUDE")
                df_resale_prices.at[index, "lng"] = res.get("LONGITUDE")
                df_resale_prices.at[index, "postal"] = res.get("POSTAL")

            break
    
    return df_resale_prices

if __name__ == "__main__":
    HEADERS = onemap_authenticate()
    df = search_resale_coordinates(HEADERS) # takes very long... (200K rows of data)