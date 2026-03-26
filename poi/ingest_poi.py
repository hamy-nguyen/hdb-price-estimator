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

def ingest_poi_csv(file_path, db_table_name):
    engine = create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    )

    df = pd.read_csv(file_path)
    df = df.drop(columns=["Unnamed: 0"])

    print(f"Ingesting: {file_path} to table {db_table_name}")

    df.to_sql(db_table_name,
              con=engine,
              if_exists="replace", 
              index=False)
    
if __name__ == "__main__":
    bus_line_file_path = _ROOT / "poi/data/bus_line.csv"
    ingest_poi_csv(bus_line_file_path, "raw_bus_line")

    bus_vol_file_path = _ROOT / "poi/data/bus_vol.csv"
    ingest_poi_csv(bus_vol_file_path, "raw_bus_vol")

    hdb_file_path = _ROOT / "poi/data/hdb.csv"
    ingest_poi_csv(hdb_file_path, "raw_hdb")

    mrt_file_path = _ROOT / "poi/data/mrt.csv"
    ingest_poi_csv(mrt_file_path, "raw_mrt")

    poi_file_path = _ROOT / "poi/data/poi.csv"
    ingest_poi_csv(poi_file_path, "raw_poi")