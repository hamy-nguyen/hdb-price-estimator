# HDB Price Estimator — Inference API

A FastAPI inference server that accepts pre-engineered HDB resale flat features and returns a predicted resale price.

---

## Project Structure

```
api/
├── app/
│   ├── main.py       # FastAPI app, lifespan model loading, endpoints
│   ├── model.py      # Model load/predict logic
│   └── schemas.py    # Pydantic request/response models
├── models/
│   └── .gitkeep      # Drop model.pkl here
├── Dockerfile
├── .dockerignore
├── .env.example
└── requirements.txt
```

---

## Setup

```bash
cd api
pip install -r requirements.txt
```

Copy the example env file if you want to override the model path:

```bash
cp .env.example .env
```

---

## Running

### Local (development)

```bash
uvicorn app.main:app --reload --port 8000
```

### Docker

```bash
docker build -t hdb-api .
docker run -p 8000:8000 -v $(pwd)/models:/app/models hdb-api
```

> **Tip:** Mount `models/` as a volume so you can swap `model.pkl` without rebuilding the image.

---

## Dummy Mode

If no `model.pkl` is found at startup, the server runs in **dummy mode** — all `/predict` calls return `500000.0`. The `/health` endpoint reports which mode is active.

To activate the real model, drop `model.pkl` into `api/models/` and restart the server.

---

## Endpoints

### `GET /health`

Returns server status and current mode.

```bash
curl http://localhost:8000/health
```

```json
{"status": "ok", "mode": "dummy"}
```

---

### `POST /predict`

Accepts all 63 pre-engineered features and returns a predicted resale price.

```bash
curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "flat_type": 4, "floor_area_sqm": 90.0, "flat_model": 2, "storey_mid": 8.0,
    "month": 3, "year": 2024, "quarter": 1, "month_index": 120,
    "max_floor_lvl": 15.0, "total_dwelling_units": 200.0,
    "has_market_hawker": false, "has_multistorey_carpark": true,
    "year_completed": 2000.0, "building_age": 24.0,
    "remaining_lease_years": 75.0, "lease_age": 25.0, "lease_age_sq": 625.0,
    "latitude": 1.35, "longitude": 103.82,
    "planning_area": 5, "region": 2, "town": 10,
    "dist_to_nearest_mrt_m": 450.0, "n_mrt_within_1km": 2,
    "dist_to_school_m": 300.0, "n_school_within_1km": 3,
    "dist_to_mall_m": 600.0, "n_mall_within_1km": 1,
    "dist_to_food_m": 150.0, "n_food_within_1km": 10,
    "dist_to_park_m": 200.0, "n_park_within_1km": 2,
    "dist_to_supermarket_m": 250.0, "n_supermarket_within_1km": 3,
    "dist_to_nearest_carpark_m": 100.0, "n_carparks_within_500m": 4,
    "gantry_height": 2.1, "car_park_decks": 3.0,
    "has_free_parking": false, "has_short_term_parking": true,
    "has_night_parking": true, "has_car_park_basement": false,
    "dist_to_nearest_bus_stop_m": 80.0, "n_bus_stop_within_1km": 15,
    "nearest_bus_stop_operating_days_per_week": 7.0, "nearest_bus_stop_busyness_level": 2,
    "dist_to_nearest_tourist_attraction_m": 2000.0,
    "transport_school_pct_bus": 0.4, "transport_school_pct_mrt": 0.3,
    "transport_school_pct_mrt_bus": 0.2, "transport_school_pct_car": 0.1,
    "tenancy_pct_owner": 0.85,
    "dwelling_pct_1room": 0.0, "dwelling_pct_2room": 0.05,
    "dwelling_pct_3room": 0.3, "dwelling_pct_4room": 0.45,
    "dwelling_pct_5room": 0.15, "dwelling_pct_exec": 0.05,
    "dwelling_pct_studio": 0.0, "dwelling_pct_multi_gen": 0.0,
    "floor_area_x_storey": 720.0, "storey_ratio": 0.53,
    "town_price_trend_6m": 0.02
  }'
```

```json
{"predicted_price": 500000.0, "is_dummy": true}
```

**Response fields:**

| Field | Type | Description |
|---|---|---|
| `predicted_price` | float | Predicted resale price in SGD |
| `is_dummy` | bool | `true` if no model is loaded (dummy mode) |

**Validation errors** (missing/out-of-range fields) return `422 Unprocessable Entity`.

---

## Interactive Docs

FastAPI auto-generates Swagger UI at:

```
http://localhost:8000/docs
```

---

## Input Features

| Group | Fields |
|---|---|
| Core | `flat_type`, `floor_area_sqm`, `flat_model`, `storey_mid`, `month`, `year`, `quarter`, `month_index` |
| Building | `max_floor_lvl`, `total_dwelling_units`, `has_market_hawker`, `has_multistorey_carpark`, `year_completed`, `building_age` |
| Lease | `remaining_lease_years`, `lease_age`, `lease_age_sq` |
| Location | `latitude`, `longitude`, `planning_area`, `region`, `town` |
| MRT | `dist_to_nearest_mrt_m`, `n_mrt_within_1km` |
| POI | `dist_to_school_m`, `n_school_within_1km`, `dist_to_mall_m`, `n_mall_within_1km`, `dist_to_food_m`, `n_food_within_1km`, `dist_to_park_m`, `n_park_within_1km`, `dist_to_supermarket_m`, `n_supermarket_within_1km` |
| Carpark | `dist_to_nearest_carpark_m`, `n_carparks_within_500m`, `gantry_height`, `car_park_decks`, `has_free_parking`, `has_short_term_parking`, `has_night_parking`, `has_car_park_basement` |
| Bus | `dist_to_nearest_bus_stop_m`, `n_bus_stop_within_1km`, `nearest_bus_stop_operating_days_per_week`, `nearest_bus_stop_busyness_level` |
| Tourist | `dist_to_nearest_tourist_attraction_m` |
| OneMap | `transport_school_pct_bus`, `transport_school_pct_mrt`, `transport_school_pct_mrt_bus`, `transport_school_pct_car`, `tenancy_pct_owner`, `dwelling_pct_*` (8 fields) |
| Interactions | `floor_area_x_storey`, `storey_ratio`, `town_price_trend_6m` |
