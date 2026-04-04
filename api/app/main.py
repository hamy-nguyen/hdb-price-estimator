import logging
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

from app import model
from app.schemas import PredictRequest, PredictResponse

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _assert_schema_matches_feature_columns() -> None:
    from app.schemas import PredictRequest
    schema_fields = set(PredictRequest.model_fields.keys())
    feature_set = set(model.FEATURE_COLUMNS)
    if schema_fields != feature_set:
        raise RuntimeError(
            f"Schema/FEATURE_COLUMNS mismatch — "
            f"extra in schema: {schema_fields - feature_set}, "
            f"extra in columns: {feature_set - schema_fields}"
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    _assert_schema_matches_feature_columns()
    model.load_model()
    yield


app = FastAPI(title="HDB Price Estimator API", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "mode": "dummy" if model._is_dummy else "live"}


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    price = model.predict(request.model_dump())
    return PredictResponse(predicted_price=price, is_dummy=model._is_dummy)
