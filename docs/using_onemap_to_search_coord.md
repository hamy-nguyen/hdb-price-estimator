## Environment Variables and Setting Up Authentication
Ensure that you have the `.env` file with the environment variables:
- ONEMAP_EMAIL
- ONEMAP_EMAIL_PASSWORD

```python
import requests
import os
from dotenv import load_dotenv

AUTH_URL = "https://www.onemap.gov.sg/api/auth/post/getToken"
PAYLOAD = {
    "email": os.environ["ONEMAP_EMAIL"],
    "password": os.environ["ONEMAP_EMAIL_PASSWORD"],
}
AUTH_RESPONSE = requests.post(AUTH_URL, json=PAYLOAD, timeout=15)

if not AUTH_RESPONSE.ok:
    print("Auth failed:", AUTH_RESPONSE.status_code)
    print(AUTH_RESPONSE.text)
    raise SystemExit

AUTH_DATA = AUTH_RESPONSE.json()
ACCESS_TOKEN = AUTH_DATA.get("access_token")
HEADERS = {"Authorization": ACCESS_TOKEN}
```

## Querying for Latitude and Longitude

```python
ONEMAP_SEARCH_URL = "https://www.onemap.gov.sg/api/common/elastic/search"
params = {"searchVal": place, "returnGeom": "Y", "getAddrDetails": "N"}

while True:
    try:
        response = requests.get(ONEMAP_SEARCH_URL, params=params, headers=HEADERS, timeout=10)
    except:
        return None
    
    # rate limited
    if response.status_code == 429:
        time.sleep(10) # 10s; change it to however long you find needed
        continue

    data = response.json()
    results = data.get("results", [])

    if not results:
        return None

    search_upper = place.strip().upper()
    match = None

    # get the closest lexicographic match from the first page
    for r in results:
        searchval = r.get("SEARCHVAL", "").strip().upper()
        if searchval == search_upper:
            match = r
            break
        if searchval.startswith(search_upper):
            match = r
            break

    # if doesn't work, just get the first result
    if match is None:
        match = results[0]

    try:
        lat = float(match.get("LATITUDE"))
        lon = float(match.get("LONGITUDE"))
        return (lat, lon)
    except Exception:
        return None
```