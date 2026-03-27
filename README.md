# hdb-price-estimator

Smart HDB Fair Value Estimation Platform: An end-to-end ML system for predicting Singapore HDB resale prices using engineered features from transaction data, geospatial context, and demographics.

## Setup Guide

### Prerequisites

- macOS with [Homebrew](https://brew.sh/)
- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager

### 1. Create and Activate Virtual Environment

```bash
uv venv
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
uv pip install apache-airflow apache-airflow-providers-mysql apache-airflow-providers-fab pymysql pandas
```

### 3. Set Up MySQL

**Start MySQL:**

```bash
brew services start mysql
```

**Log in and create the database and user:**

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

**Verify:**

```bash
mysql -u bt4301 -p -e "SHOW DATABASES;"
```

### 4. Configure Airflow

Set `AIRFLOW_HOME` to the project directory (add to `~/.zshrc` for persistence):

```bash
export AIRFLOW_HOME=/Users/hamynguyen/Documents/School/Notes/BT4301/hdb-price-estimator
```

Run the database migration:

```bash
airflow db migrate
```

Edit `airflow.cfg` and update these settings:

```ini
[core]
dags_folder = /Users/hamynguyen/Documents/School/Notes/BT4301/hdb-price-estimator/airflow/dags
load_examples = False
execution_api_server_url = http://localhost:8081/execution/

[api]
base_url = http://localhost:8081
port = 8081

[dag_processor]
dag_bundle_config_list = [{"name": "dags-folder", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {"path": "/Users/hamynguyen/Documents/School/Notes/BT4301/hdb-price-estimator/airflow/dags"}}]
refresh_interval = 10
```

### 5. Start Airflow

```bash
source .venv/bin/activate
export AIRFLOW_HOME=/Users/hamynguyen/Documents/School/Notes/BT4301/hdb-price-estimator
airflow standalone
```

Login credentials are stored in `simple_auth_manager_passwords.json.generated` in the project root.

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

| Task | Description |
|------|-------------|
| `drop_tables_before_ingest` | Drops all target tables (full refresh) |
| `extract_<source>` | Fetches data per source (API or local CSV) |
| `load_<source>` | Loads extracted data into MySQL |

### Data Sources

| Table | Source | Type |
|-------|--------|------|
| `tourist_attractions` | data.gov.sg API | poll-download |
| `carpark_data` | data.gov.sg API | datastore_search |
| `resale_flat_price` | data.gov.sg API | poll-download |
| `hdb` | `dataset/hdb.csv` | Local CSV |
| `poi` | `dataset/poi.csv` | Local CSV |
| `bus_vol` | `dataset/bus_vol.csv` | Local CSV |
| `bus_line` | `dataset/bus_line.csv` | Local CSV |
| `mrt` | `dataset/mrt.csv` | Local CSV |

Source configurations are defined in `tourist_attraction_ingest/dag_helpers.py` → `SOURCES`.

## Troubleshooting

- **MySQL won't start:** `pkill -f mysqld`, remove stale pid file from `/opt/homebrew/var/mysql/`, then `brew services start mysql`
- **`airflow: command not found`:** Make sure the venv is activated: `source .venv/bin/activate`
- **`conn_id mysql_default isn't defined`:** Add the connection per step 6
- **`Access denied (using password: NO)`:** Delete and re-add the connection with the password in the URI
- **Reset MySQL password:** `mysql -u root -p` then `ALTER USER 'bt4301'@'localhost' IDENTIFIED BY 'new_password';`
