# MySQL Setup for Tourist Attractions Ingest DAG

Step-by-step instructions to set up MySQL for the tourist attractions pipeline.

---

## 1. Install MySQL Server

### Ubuntu/Debian

```bash
sudo apt update
sudo apt install mysql-server -y
```

### macOS (Homebrew)

```bash
brew install mysql
brew services start mysql
```

---

## 2. Secure MySQL (Ubuntu/Debian)

Run the security script:

```bash
sudo mysql_secure_installation
```

- Set root password: **Yes**
- Remove anonymous users: **Yes**
- Disallow root login remotely: **Yes** (unless needed remotely)
- Remove test database: **Yes**
- Reload privilege tables: **Yes**

---

## 3. Create Database and User

Log in to MySQL:

```bash
sudo mysql -u root -p
```

Then run:

```sql
-- Create database for the pipeline
CREATE DATABASE airflow_data;

-- Create a dedicated user (replace 'your_password' with a strong password)
CREATE USER 'airflow_user'@'localhost' IDENTIFIED BY 'your_password';

-- Grant privileges
GRANT ALL PRIVILEGES ON airflow_data.* TO 'airflow_user'@'localhost';
FLUSH PRIVILEGES;

-- Verify
SHOW DATABASES;
SELECT user, host FROM mysql.user WHERE user = 'airflow_user';

EXIT;
```

---

## 4. Install Airflow MySQL Provider

Airflow requires the MySQL provider for the MySQL connection type to appear:

```bash
pip install apache-airflow-providers-mysql
```

Restart Airflow after installing.

---

## 5. Add Airflow Connection

### Option A: Airflow UI

1. Open Airflow UI: http://localhost:8081
2. Go to **Admin** → **Connections**
3. Click **+** (Add)
4. Fill in:

   | Field    | Value                |
   |----------|----------------------|
   | Connection Id | `mysql_default`   |
   | Connection Type | `MySQL`       |
   | Host     | `localhost` (or `127.0.0.1`) |
   | Schema   | `airflow_data`      |
   | Login    | `airflow_user`      |
   | Password | `your_password`     |
   | Port     | `3306`              |

5. Click **Save**

### Option B: Airflow CLI

```bash
airflow connections add mysql_default \
  --conn-type mysql \
  --conn-host localhost \
  --conn-schema airflow_data \
  --conn-login airflow_user \
  --conn-password 'your_password' \
  --conn-port 3306
```

### Option C: Environment variable

Add to `~/.bashrc` or `airflow.cfg` env section:

```bash
export AIRFLOW_CONN_MYSQL_DEFAULT="mysql://airflow_user:your_password@localhost:3306/airflow_data"
```

---

## 5. Install Python MySQL Driver

Activate your virtual environment and install:

```bash
source venv/bin/activate   # or: source ~/python3venv/bin/activate
pip install pymysql
```

---

## 6. Verify MySQL Access

Test from Python:

```bash
python -c "
import pymysql
conn = pymysql.connect(
    host='localhost',
    user='airflow_user',
    password='your_password',
    database='airflow_data',
)
print('Connected successfully')
conn.close()
"
```

---

## 7. Run the DAG

1. Restart Airflow (scheduler, dag-processor, api-server) if needed
2. Unpause the DAG `tourist_attractions_ingest` in the Airflow UI
3. Trigger a run or wait for the daily schedule
4. Check the `tourist_attractions` table:

```sql
mysql -u airflow_user -p airflow_data -e "SELECT COUNT(*) FROM tourist_attractions;"
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Access denied for user` | Check username, password, and `GRANT` |
| `Can't connect to MySQL server` | Ensure `mysql` service is running: `sudo systemctl status mysql` |
| `Unknown database 'airflow_data'` | Run `CREATE DATABASE airflow_data;` |
| `No module named 'pymysql'` | `pip install pymysql` in the venv Airflow uses |
| DAG not loading | Add project root to `sys.path` (already in DAG file) |

---

## Table Schema (Auto-Created)

The DAG creates the `tourist_attractions` table with columns aligned to the API:

- `objectid_1`, `url_path`, `image_path`, `image_alt_text`, `photocredits`
- `pagetitle`, `lastmodified`, `latitude`, `longitude`, `address`, `postalcode`
- `overview`, `external_link`, `meta_description`, `opening_hours`
- `inc_crc`, `fmel_upd_d`, `longitude`, `latitude`

All text fields use `TEXT`; numeric/date fields use appropriate MySQL types.
