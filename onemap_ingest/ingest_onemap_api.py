import os
import requests
import pandas as pd

# Set-up authentication for OneMap API

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

PLANNING_AREA_URL = "https://www.onemap.gov.sg/api/public/popapi/getPlanningareaNames"

def get_planning_areas():
    '''
    Fetches planning areas over the years from the OneMap API and saves them to a CSV file.
    '''
    years = [1998, 2008, 2014, 2019]

    all_planning_areas = []

    for year in years:
        params = {"year": year}
        response = requests.get(PLANNING_AREA_URL, params=params, headers=HEADERS, timeout=15)
        data = response.json()

        for planning_area in data['SearchResults']:
            all_planning_areas.append({
                "planning_area": planning_area['pln_area_n'],
                "year": year
            })
        
    planning_areas_df = pd.DataFrame(all_planning_areas)
    planning_areas_df.to_csv("dataset/planning_areas.csv", index=False)

    return planning_areas_df

if __name__ == "__main__":
    get_planning_areas()