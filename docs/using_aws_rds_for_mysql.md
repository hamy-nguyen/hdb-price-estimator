## Environment Variables
Ensure that you have the `.env` file with the environment variables:
- AWS_RDS_HOST
- AWS_RDS_PORT
- AWS_RDS_USER
- AWS_RDS_PASSWORD
- AWS_RDS_DB

```python
import os

host = os.environ["AWS_RDS_HOST"]
port = os.environ["AWS_RDS_PORT"]
user = os.environ["AWS_RDS_USER"]
password = os.environ["AWS_RDS_PASSWORD"]
db = os.environ["AWS_RDS_DB"]
```

## Connecting to the MySQL Database
```python
from sqlalchemy import create_engine

engine = create_engine(
    f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
)
```

## Writing Data into the MySQL Database
```python
import pandas as pd

df = pd.read_csv("dataset/onemap_planning_areas.csv")

df.to_sql(
    "raw_planning_areas", # table name
    con=engine,           # must be defined earlier
    if_exists="replace",  # or append
    index=False
)
```

## Reading Data from the MySQL Database
```python
import pandas as pd

df = pd.read_sql(
    "SELECT * FROM raw_planning_areas",
    con=engine
)
```

## Connecting via VS Code (SQLTools + MySQL)
1. Add New Connection
2. Select MySQL
3. Fill in the following information (the rest as defaults):
    ```
    Connection Name: rds-mysql (or anything really)
    Server Address: hdb-price-estimator.c7so2qk8smgw.ap-southeast-1.rds.amazonaws.com
    Port: 3306
    Database: hdb-price-estimator
    Username: admin
    Password mode: Ask on connect
    ```
4. Test/Save Connection and fill the password (found in the `.env` file)