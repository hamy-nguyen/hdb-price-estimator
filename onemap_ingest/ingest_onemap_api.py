import os
import requests

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
    Fetches planning areas over the years from the OneMap API.
    '''
    years = [1998, 2008, 2014, 2019]

    all_data = []

    for year in years:
        params = {"year": year}
        response = requests.get(PLANNING_AREA_URL, params=params, headers=HEADERS, timeout=15)
        data = response.json()

        for planning_area in data['SearchResults']:
            all_data.append({
                "planning_area": planning_area['pln_area_n'],
                "year": year
            })

    return all_data