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
├── web_application/
│   ├── streamlit.py               # Dashboard: map, month filter, hosted /predict estimate
│   └── predict_api_params.py      # OneMap + nearest-row payload builder for the API
├── scripts/
│   ├── extract_onemap.py          # Extract demographics from OneMap API
│   ├── onemap_address_search.py   # OneMap elastic search (used by predict_api_params)
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

### 1. Create and Activate Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
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

Set `AIRFLOW_HOME` to the project's airflow directory:

```bash
export AIRFLOW_HOME=/path/to/hdb-price-estimator/airflow
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

In separate terminals (with venv activated and `AIRFLOW_HOME` exported):

Option 1:
```bash
airflow standalone
```

Option 2:
```bash
airflow scheduler &
airflow dag-processor &
airflow api-server
```

Login credentials are printed on first startup and stored in `airflow/simple_auth_manager_passwords.json.generated`.

Open the UI at http://localhost:8081.

### 6. Start MLflow

MLflow is used to track model training runs. Start the tracking server before triggering the pipeline:

```bash
python -m mlflow server --host 127.0.0.1 --port 9080
```

Leave this running in a separate terminal. The MLflow UI will be available at http://localhost:9080.

### 7. Add MySQL Connection in Airflow

In a separate terminal (with venv activated and `AIRFLOW_HOME` exported):

```bash
airflow connections add mysql_default \
  --conn-uri "mysql://bt4301:your_password@localhost:3306/HDB_Data"
```

Replace `your_password` with the password you set in step 3.

### 8. Trigger the Pipeline

The `data_ingest` DAG runs automatically on the first of every month. To trigger manually via the Airflow UI:

1. Open http://localhost:8081
2. Toggle on the `data_ingest` DAG
3. Click the play button to trigger a run

Or via CLI:

```bash
airflow dags unpause data_ingest
airflow dags trigger data_ingest
```

Triggering `data_ingest` is all that is needed — the remaining DAGs chain automatically as described below.

## DataOps Pipeline

The pipeline is fully automated across four Airflow DAGs that chain sequentially. Triggering `data_ingest` kicks off the entire flow:

```
data_ingest (@monthly)
  └──► data_clean
         └──► data_transform
                └──► data_train
```

### data_ingest

**DAG:** `airflow/dags/ingest_dag.py` · **Schedule:** `@monthly` (1st of each month)

Pulls raw data from data.gov.sg APIs and local CSV files into MySQL `raw_*` tables. Static datasets (HDB block info, MRT stations, POIs, bus stops, OneMap demographics, etc.) are ingested once and skipped on subsequent runs. Resale flat price data is ingested **incrementally** — only the previous calendar month is fetched on each run, so re-running the DAG never duplicates existing data.

**Data integrity** is verified at every stage using SHA-256 row fingerprinting. After each upsert, every row's data columns are re-hashed and compared against the stored `_fp` column. Any mismatch triggers an automatic retry (up to 3 attempts) before the task fails. This ensures the data written to MySQL exactly matches what was extracted from the source.

### data_clean

**DAG:** `airflow/dags/clean_dag.py` · **Schedule:** triggered by `data_ingest`

Reads each `raw_*` table and applies source-specific cleaning rules (standardising column names and types, removing duplicates, converting coordinate systems, filtering out-of-bounds values, etc.) before writing to `clean_*` tables. All sources are cleaned in parallel. Fingerprint verification runs after each write.

### data_transform

**DAG:** `airflow/dags/transform_dag.py` · **Schedule:** triggered by `data_clean`

Joins all cleaned tables onto the resale flat price records through a sequential chain of geospatial enrichment steps:

```
joinable_resale_prices → join_hdb → join_mrt → join_poi → join_onemap → join_car_park → join_bus → join_tourist_attractions → transform_resale_prices
```

Each step uses Haversine / BallTree spatial matching to compute distance and count features (e.g. `dist_to_nearest_mrt_m`, `n_mrt_within_1km`) and demographic features from OneMap. The final output is `transform_resale_flat_price`, a single enriched table with 16 model-ready features per transaction.

Fingerprint verification runs after each join step.

### pipeline_tracking

A `pipeline_tracking` table in MySQL records the processing state of every resale month:

| Column | Description |
|--------|-------------|
| `month` | Calendar month (`YYYY-MM`) |
| `is_ingested` | Set to `True` after successful ingestion |
| `is_cleaned` | Set to `True` after successful cleaning |
| `is_transformed` | Set to `True` after successful transformation |

Before each stage, the DAG checks `pipeline_tracking` and skips any month already marked as processed. This means re-triggering a DAG mid-month is safe — already-processed months are not reprocessed. For example, if today is 11 April 2026, only the March 2026 data will be processed; prior months already present in the table are skipped.

## MLOps — Model Training

### data_train

**DAG:** `airflow/dags/train_dag.py` · **Schedule:** triggered by `data_transform`

Trains three candidate models against `transform_resale_flat_price` using an 80/20 train/test split:

| Model | Notes |
|-------|-------|
| Linear Regression | Baseline |
| Ridge Regression | L2-regularised linear model |
| XGBoost | Gradient-boosted trees |

Each model is wrapped in a scikit-learn `Pipeline` (median imputation → standard scaling → model) so that all preprocessing is bundled inside the saved artefact.

Every run is logged to MLflow (experiment: **`HDB Resale Price Prediction: Auto Training`**) with metrics (RMSE, MAE, MAPE, R²) and the full pipeline artefact. The run with the lowest test RMSE is tagged `best_model=true` in MLflow for easy identification in the UI.

The winning pipeline is saved as a pickle to:

```
api/models/model.pkl
```

After saving, the DAG calls `POST /reload-model` on the FastAPI server so the live API hot-swaps to the new model without a restart.

### Viewing MLflow Results

With the MLflow server running at http://localhost:9080, open the **`HDB Resale Price Prediction: Auto Training`** experiment to compare all runs across months, inspect per-model metrics, and identify which model won each retraining cycle.

## DevOps - Application

### Backend (FastAPI)

The inference API serves predictions from the trained model pickle at `api/models/model.pkl`.

```bash
cd api
uvicorn app.main:app --host 0.0.0.0 --port 7860 --reload
```

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Returns `{"status": "ok", "mode": "live"\|"dummy"}` |
| `/predict` | POST | Accepts 16 features, returns predicted resale price |
| `/reload-model` | POST | Hot-swaps the in-memory model from disk (called automatically by `data_train`) |

If `api/models/model.pkl` is not found, the API starts in **dummy mode** and returns a fixed placeholder price of $500,000 until a model is available.

### Frontend (Streamlit)

```bash
streamlit run web_application/streamlit.py
```

Open http://localhost:8501 in your browser.

The dashboard shows an interactive map of resale transactions coloured by price for the selected month. The price estimator form accepts a postal code and flat details, geocodes the address via OneMap, looks up nearby HDB location features, and calls the local `/predict` API.

**OneMap credentials** are required for the price estimator. Add to a `.env` file at the repository root:

```env
ONEMAP_EMAIL=your_onemap_account_email
ONEMAP_EMAIL_PASSWORD=your_onemap_password
```

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
