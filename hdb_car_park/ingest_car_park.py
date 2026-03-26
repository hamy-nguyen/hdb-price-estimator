import requests
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Repo root
_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env")

# AWS RDS MySQL credentials
host = os.environ["AWS_RDS_HOST"]
port = os.environ["AWS_RDS_PORT"]
user = os.environ["AWS_RDS_USER"]
password = os.environ["AWS_RDS_PASSWORD"]
db = os.environ["AWS_RDS_DB"]

# data.gov.sg API keys and headers
DATA_GOV_API_KEY = os.getenv("DATA_GOV_API_KEY")
HEADERS = {"X-Api-Key": DATA_GOV_API_KEY}
DATASTORE_API = "https://data.gov.sg/api/action/datastore_search"
DATASET_ID = "d_23f946fa557947f93a8043bbef41dd09"

def ingest_hdb_car_park ():
    engine = create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    )
        
    limit = 100
    offset = 0

    all_data = []

    while True:
        params = {
            "resource_id": DATASET_ID,
            "offset": offset
        }

        response = requests.get(DATASTORE_API, headers=HEADERS, params=params).json()
        records = response["result"]["records"]

        if not records:
            break

        df = pd.DataFrame(records)
        all_data.append(df)

        offset += limit
        print(f"Fetched {offset}")

    final_df = pd.concat(all_data, ignore_index=True)

    print("Done:", len(final_df))
    print(final_df)

    print(f"Ingesting HDB car park data to MySQL table raw_car_park...")
    final_df.to_sql("raw_car_park",
                    con=engine,
                    if_exists="replace", 
                    index=False) 

if __name__ == "__main__":
    ingest_hdb_car_park()