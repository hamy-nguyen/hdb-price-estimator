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
# DATASTORE_API = "https://data.gov.sg/api/action/datastore_search"
# DATASET_ID = "d_0f2f47515425404e6c9d2a040dd87354s"

def ingest_tourist_attractions():
    pass

if __name__ == "__main__":
    ingest_tourist_attractions()