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

Copy the example env file and set your model path:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `MODEL_PATH` | `models/model.pkl` | Path to the model pickle file, relative to the `api/` directory |

Example:
```
MODEL_PATH=models/xgb_model_log.pkl
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
    "flat_model": 2,
    "floor_area_sqm": 90.0,
    "max_floor_lvl": 15.0,
    "total_dwelling_units": 200.0,
    "storey_mid": 8.0,
    "remaining_lease_years": 75.0,
    "town": 10,
    "dist_to_nearest_mrt_m": 450.0,
    "n_mrt_within_1km": 2,
    "dist_to_nearest_bus_stop_m": 80.0,
    "n_bus_stop_within_1km": 15,
    "month_index": 120,
    "dist_to_food_m": 150.0,
    "n_food_within_1km": 10,
    "dist_to_supermarket_m": 250.0,
    "n_supermarket_within_1km": 3
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

## Deploying to Hugging Face Spaces

Deployment is **automated via GitHub Actions** (`.github/workflows/deploy-hf.yml`). Every push to `main` that touches any file under `api/` automatically redeploys the Space — no manual steps needed.

**How it works:**
1. Push changes to `main` (e.g. updated model, code fix)
2. GitHub Actions checks out the repo and runs `git subtree push --prefix=api` to the HF Space
3. HF auto-rebuilds the Docker image and restarts the Space

**One-time setup — add HF token as a GitHub secret:**
1. Generate a **write** access token at huggingface.co → Settings → Access Tokens
2. In the GitHub repo → Settings → Secrets and variables → Actions → **New repository secret**
3. Name: `HF_TOKEN`, Value: your token

**Manual redeploy** (if needed, run from repo root):
```bash
git subtree push --prefix=api huggingface main
```

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
| Flat | `flat_model`, `floor_area_sqm`, `storey_mid` |
| Building | `max_floor_lvl`, `total_dwelling_units` |
| Lease | `remaining_lease_years` |
| Location | `town` |
| Time | `month_index` |
| MRT | `dist_to_nearest_mrt_m`, `n_mrt_within_1km` |
| Bus | `dist_to_nearest_bus_stop_m`, `n_bus_stop_within_1km` |
| Food | `dist_to_food_m`, `n_food_within_1km` |
| Supermarket | `dist_to_supermarket_m`, `n_supermarket_within_1km` |
