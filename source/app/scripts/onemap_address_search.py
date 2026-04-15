"""
OneMap: postal code (or any search text) → address details + coordinates.

Uses the same email/password token as `extract_onemap.py`.
Search API docs: https://www.onemap.gov.sg/apidocs/search/

Environment (repo root `.env`):
  ONEMAP_EMAIL
  ONEMAP_EMAIL_PASSWORD
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env")

AUTH_URL = "https://www.onemap.gov.sg/api/auth/post/getToken"
SEARCH_URL = "https://www.onemap.gov.sg/api/common/elastic/search"


def get_onemap_access_token() -> str:
    """POST email/password → `access_token` for Authorization header."""
    email = os.environ.get("ONEMAP_EMAIL", "").strip()
    password = os.environ.get("ONEMAP_EMAIL_PASSWORD", "").strip().strip("'\"")
    if not email or not password:
        raise RuntimeError("Set ONEMAP_EMAIL and ONEMAP_EMAIL_PASSWORD in .env")
    r = requests.post(
        AUTH_URL,
        json={"email": email, "password": password},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {data}")
    return str(token)


def search_onemap(
    search_val: str,
    *,
    return_geom: bool = True,
    get_addr_details: bool = True,
    page_num: int = 1,
    token: str | None = None,
) -> dict[str, Any]:
    """
    Call GET /api/common/elastic/search.

    `search_val` can be a 6-digit postal code (e.g. "200640"), block+street, building name, etc.

    Response shape (typical success):
        {
            "found": int,
            "totalNumPages": int,
            "pageNum": int,
            "results": [
                {
                    "SEARCHVAL": str,
                    "BLK_NO": str,
                    "ROAD_NAME": str,
                    "BUILDING": str,
                    "ADDRESS": str,
                    "POSTAL": str,
                    "X": str,              # SVY21
                    "Y": str,
                    "LATITUDE": str,
                    "LONGITUDE": str,
                    "LONGTITUDE": str,    # typo preserved by API
                    ...
                },
                ...
            ]
        }
    """
    tok = token or get_onemap_access_token()
    params = {
        "searchVal": search_val.strip(),
        "returnGeom": "Y" if return_geom else "N",
        "getAddrDetails": "Y" if get_addr_details else "N",
        "pageNum": page_num,
    }
    r = requests.get(
        SEARCH_URL,
        params=params,
        headers={"Authorization": tok},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def postal_code_to_addresses(postal_code: str) -> list[dict[str, Any]]:
    """Normalize 6-digit SG postal and return the `results` list (may be empty)."""
    digits = "".join(c for c in postal_code.strip() if c.isdigit())
    if len(digits) != 6:
        raise ValueError(f"Expected 6-digit postal code, got: {postal_code!r}")
    data = search_onemap(digits)
    return list(data.get("results") or [])


if __name__ == "__main__":
    import json
    import sys

    q = sys.argv[1] if len(sys.argv) > 1 else "200640"
    out = search_onemap(q)
    print(json.dumps(out, indent=2))
