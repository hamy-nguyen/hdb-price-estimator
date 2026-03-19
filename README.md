# hdb-price-estimator
Smart HDB Fair Value Estimation Platform: An end-to-end ML system for predicting Singapore HDB resale prices using engineered features from transaction data, geospatial context, and demographics.

## Ingesting Tourist Attractions Data into MySQL

The project includes an Airflow DAG that extracts tourist attractions from the data.gov.sg API and loads them into MySQL.

### Prerequisites

- MySQL server running
- Airflow installed and running (scheduler, dag-processor, api-server)
- Dependencies: `pip install -r requirements.txt`

### Quick Start

1. **Set up MySQL** — Create database, user, and grant privileges. See [`tourist_attraction_ingest/MYSQL_SETUP.md`](tourist_attraction_ingest/MYSQL_SETUP.md) for step-by-step instructions.

2. Configure Airflow Connection

In the Airflow UI: **Admin** → **Connections** → Add connection **mysql_default**:

| Field | Value |
|-------|-------|
| Connection Type | MySQL |
| Host | localhost |
| Schema | HDB_Data |
| Login | airflow_user |
| Password | your_password |
| Port | 3306 |


3. **Install MySQL provider** (if MySQL type is missing):
   ```bash
   pip install apache-airflow-providers-mysql
   ```

4. **Run the DAG** — Unpause `tourist_attractions_ingest` in the Airflow UI and trigger a run, or wait for the daily schedule.

5. **Verify** — Check the `tourist_attractions` table:
   ```bash
   mysql -u airflow_user -p HDB_Data -e "SELECT COUNT(*) FROM tourist_attractions;"
   ```

## Tourist Attractions: Ingest Data into MySQL

This project includes an Airflow pipeline that extracts Singapore tourist attractions from the [data.gov.sg](https://data.gov.sg) API and loads them into MySQL.

### Prerequisites

- MySQL server running with a database and user (see `tourist_attraction_ingest/MYSQL_SETUP.md` for setup)
- Apache Airflow installed and configured
- Dependencies: `pip install -r requirements-tourist-ingest.txt`

### Quick Start

1. **Set up MySQL** (if not done): Create database `HDB_Data`, user `airflow_user`, and grant privileges. Full steps in `tourist_attraction_ingest/MYSQL_SETUP.md`.

2. **Add Airflow connection**: Admin → Connections → Add  
   - Connection Id: `mysql_default`  
   - Connection Type: MySQL  
   - Host: `localhost`  
   - Schema: `HDB_Data`  
   - Login: `airflow_user`  
   - Password: *password*  
   - Port: `3306`

3. **Start Airflow** (if not running):
   ```bash
   airflow scheduler &
   airflow dag-processor &
   airflow api-server
   ```

4. **Run the DAG**: Open http://localhost:8081 → DAGs → `tourist_attractions_ingest` → Unpause → Trigger Run.

5. **Verify**: After the run completes, check the `tourist_attractions` table:
   ```bash
   mysql -u airflow_user -p HDB_Data -e "SELECT COUNT(*) FROM tourist_attractions;"
   ```

### Pipeline Overview

| Task | Description |
|------|-------------|
| `extract_tourist_attractions` | Fetches data from data.gov.sg API, parses CSV/JSON |
| `load_to_mysql` | Loads extracted data into the `tourist_attractions` table (full refresh) |

Schedule: daily (`@daily`).