# hdb-price-estimator

Smart HDB Fair Value Estimation Platform: An end-to-end ML system for predicting Singapore HDB resale prices using engineered features from transaction data, geospatial context, and demographics.

## Project Structure

```
hdb-price-estimator/
├── airflow/dags/
│   ├── ingest_dag.py              # Airflow DAG definition
│   └── helpers/
│       ├── dag_helpers.py         # Extract, upsert, verify, watermark logic
│       └── data_watermarking.py   # SHA-256 row fingerprinting
├── notebooks/
│   ├── datapipeline.ipynb         # ETL: transform raw tables → analytics tables
│   ├── eda_resale_flat_price.ipynb          # Data profiling, cleaning & EDA
│   ├── eda_tourist_attractions.ipynb
│   ├── eda_places_of_interest.ipynb
│   ├── feature_engineering_resale_flat_price.ipynb  # Feature engineering
│   └── tourist_attraction_cleaning.ipynb
├── scripts/
│   ├── extract_onemap.py          # Extract demographics from OneMap API
│   └── search_coord.py            # Geocode HDB addresses via OneMap
├── dataset/
│   ├── raw/                       # Original CSV source files
│   └── processed/                 # Cleaned/derived datasets
├── .gitignore
├── requirements.txt
└── README.md
```

**Rationale:** Files are grouped by function — DAG code under `airflow/dags/`, all notebooks in `notebooks/`, standalone scripts in `scripts/`, and source data separated into `raw/` vs `processed/`. Previously, files were scattered across directories named by origin (`tourist_attraction_ingest/`, `etl_notebooks/`, `onemap/`) which was misleading since the pipeline ingests 13 sources, not just tourist attractions.

## Setup Guide

### Prerequisites

- Python 3.11+
- MySQL 8.0+
- [uv](https://github.com/astral-sh/uv) package manager (or `pip`)

### 1. Create and Activate Virtual Environment

```bash
uv venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows
```

### 2. Install Dependencies

```bash
uv pip install apache-airflow apache-airflow-providers-mysql apache-airflow-providers-fab pymysql pandas
```

Or with pip:

```bash
pip install apache-airflow apache-airflow-providers-mysql apache-airflow-providers-fab pymysql pandas
```

### 3. Set Up MySQL

Start the MySQL server:

```bash
# macOS (Homebrew)
brew services start mysql

# Linux (systemd)
sudo systemctl start mysql

# Windows
net start mysql
```

Log in as root and create the database and user:

```bash
mysql -u root -p
```

```sql
CREATE DATABASE HDB_Data;
CREATE USER 'bt4301'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON HDB_Data.* TO 'bt4301'@'localhost';
FLUSH PRIVILEGES;
EXIT;
```

Verify:

```bash
mysql -u bt4301 -p -e "SHOW DATABASES;"
```

### 4. Configure Airflow

Set `AIRFLOW_HOME` to the project directory:

```bash
export AIRFLOW_HOME=/path/to/hdb-price-estimator
```

Add this to your shell profile (`~/.bashrc`, `~/.zshrc`, etc.) for persistence.

Run the database migration:

```bash
airflow db migrate
```

Edit `airflow.cfg` (generated in `AIRFLOW_HOME`) and update these settings:

```ini
[core]
dags_folder = /path/to/hdb-price-estimator/airflow/dags
load_examples = False
execution_api_server_url = http://localhost:8081/execution/

[api]
base_url = http://localhost:8081
port = 8081

[dag_processor]
dag_bundle_config_list = [{"name": "dags-folder", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {"path": "/path/to/hdb-price-estimator/airflow/dags"}}]
refresh_interval = 10
```

Replace `/path/to/hdb-price-estimator` with your actual project path.

### 5. Start Airflow

```bash
source .venv/bin/activate
export AIRFLOW_HOME=/path/to/hdb-price-estimator
airflow standalone
```

Login credentials are printed on first startup and stored in `simple_auth_manager_passwords.json.generated`.

### 6. Add MySQL Connection in Airflow

In a separate terminal (with venv activated and `AIRFLOW_HOME` exported):

```bash
airflow connections add mysql_default \
  --conn-uri "mysql://bt4301:your_password@localhost:3306/HDB_Data"
```

Replace `your_password` with the password you set in step 3.

### 7. Run the DAG

1. Open http://localhost:8081
2. Toggle on the `data_ingest` DAG
3. Click the play button to trigger a run

Or via CLI:

```bash
airflow dags unpause data_ingest
airflow dags trigger data_ingest
```

## Pipeline Overview

DAG id: **`data_ingest`** · file: **`airflow/dags/ingest_dag.py`** · schedule: `@daily`

### DAG Flow

```
verify_data_integrity → [extract_<source> → watermark_<source> → upsert_<source>] (per source)
```

| Task | Description |
|------|-------------|
| `verify_data_integrity` | Reads existing tables, recomputes SHA-256 fingerprints, logs any tampered rows |
| `extract_<source>` | Fetches data from API or local CSV |
| `watermark_<source>` | Adds per-row SHA-256 fingerprint (`_fp` column) |
| `upsert_<source>` | Inserts new rows, updates changed rows, preserves untouched rows |

### Why Upsert Instead of Full Refresh

The original pipeline used a **full refresh** (drop all tables → reload from scratch). This had two problems:

1. **Watermarking was meaningless.** Fingerprints were recalculated every run, so if someone tampered with data in MySQL between runs, it was silently overwritten — tampering was never detected.
2. **Unnecessary downtime.** Tables were dropped and recreated even when data hadn't changed, making them temporarily unavailable to downstream queries.

The reworked pipeline uses **upsert** (`INSERT ... ON DUPLICATE KEY UPDATE`):
- **New rows** are inserted.
- **Changed rows** are updated (including a new fingerprint).
- **Unchanged rows** are left alone — their fingerprints persist.

This makes the `verify_data_integrity` step meaningful: it runs before each ingestion, recomputes fingerprints from the stored data, and compares them against the stored `_fp` values. Any mismatch means someone modified data directly in MySQL, and the DAG logs a `WARNING` with the affected table, row count, and row indices.

### How Watermarking Works

Each row gets a SHA-256 fingerprint (`_fp` column) computed from all its data columns:

1. Column values are serialized into canonical JSON (sorted keys, deterministic float rendering).
2. The JSON string is hashed with SHA-256.
3. The hash is stored alongside the row in MySQL.

To detect tampering: recompute the hash from the data columns and compare it to the stored `_fp`. If they differ, the row was modified outside the pipeline.

**Limitation:** The hashing algorithm is public (in `data_watermarking.py`), so a malicious actor with database and code access could recompute valid fingerprints after tampering. For stronger guarantees, HMAC with a secret key would be needed. The current approach is designed to detect accidental corruption or unauthorized edits by users who don't have access to the codebase.

### Data Sources

| Table | Source | Type |
|-------|--------|------|
| `raw_tourist_attractions` | data.gov.sg API | poll-download |
| `raw_carpark` | data.gov.sg API | datastore_search |
| `raw_resale_flat_price` | data.gov.sg API | poll-download |
| `raw_hdb` | `dataset/raw/hdb.csv` | Local CSV |
| `raw_poi` | `dataset/raw/poi.csv` | Local CSV |
| `raw_bus_vol` | `dataset/raw/bus_vol.csv` | Local CSV |
| `raw_bus_line` | `dataset/raw/bus_line.csv` | Local CSV |
| `raw_mrt` | `dataset/raw/mrt.csv` | Local CSV |
| `raw_onemap_planning_areas` | `dataset/raw/onemap_planning_areas.csv` | Local CSV |
| `raw_onemap_transport_school` | `dataset/raw/onemap_transport_to_school.csv` | Local CSV |
| `raw_onemap_transport_work` | `dataset/raw/onemap_transport_to_work.csv` | Local CSV |
| `raw_onemap_tenancy` | `dataset/raw/onemap_tenancy.csv` | Local CSV |
| `raw_onemap_dwelling` | `dataset/raw/onemap_dwelling.csv` | Local CSV |

Source configurations are defined in `airflow/dags/helpers/dag_helpers.py` → `SOURCES`.

Each table has a defined **primary key** with VARCHAR lengths sized to actual data (verified against source datasets with headroom). See `PRIMARY_KEYS` in `dag_helpers.py` for the full mapping.

## Troubleshooting

- **MySQL won't start:** Kill stale processes (`pkill -f mysqld`), remove any stale `.pid` files from the MySQL data directory, then restart.
- **`airflow: command not found`:** Make sure the venv is activated: `source .venv/bin/activate`
- **`conn_id mysql_default isn't defined`:** Add the connection per step 6.
- **`Access denied (using password: NO)`:** Delete and re-add the connection with the password in the URI:
  ```bash
  airflow connections delete mysql_default
  airflow connections add mysql_default --conn-uri "mysql://bt4301:your_password@localhost:3306/HDB_Data"
  ```
- **`Specified key was too long`:** Drop the database (`DROP DATABASE HDB_Data; CREATE DATABASE HDB_Data;`) and re-trigger the DAG. This happens if tables were created with an older schema.
- **Reset MySQL password:** `mysql -u root -p` then `ALTER USER 'bt4301'@'localhost' IDENTIFIED BY 'new_password';`
