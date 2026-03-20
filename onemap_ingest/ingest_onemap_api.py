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