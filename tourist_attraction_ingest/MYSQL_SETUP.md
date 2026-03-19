# MySQL Setup for Tourist Attractions Ingest DAG

Step-by-step instructions to set up MySQL for the tourist attractions pipeline.

---

## 1. Install MySQL Server 
- Follow the steps from: Supplementary Learning Activities for Week 07
---

## 2. Create Database and User

Log in to MySQL:

```bash
sudo mysql -u root -p
```

Then run:

```sql
-- Create database for the pipeline
CREATE DATABASE HDB_Data;

-- Create a dedicated user (replace 'your_password' with a strong password)
CREATE USER 'airflow_user'@'localhost' IDENTIFIED BY 'your_password';

-- Grant privileges
GRANT ALL PRIVILEGES ON HDB_Data.* TO 'airflow_user'@'localhost';
FLUSH PRIVILEGES;

-- Verify
SHOW DATABASES;
SELECT user, host FROM mysql.user WHERE user = 'airflow_user';

EXIT;
```

---

## 3. Install Airflow MySQL Provider

Airflow requires the MySQL provider for the MySQL connection type to appear:

```bash
pip install apache-airflow-providers-mysql
```

Restart Airflow after installing.

---

## 4. Add Airflow Connection

### Airflow UI

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
