from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    flat_model: str
    floor_area_sqm: float = Field(..., gt=0, le=500)
    max_floor_lvl: float = Field(..., ge=1)
    total_dwelling_units: float = Field(..., ge=0)
    storey_mid: float = Field(..., ge=1)
    remaining_lease_years: float = Field(..., ge=0, le=99)
    town: str
    dist_to_nearest_mrt_m: float = Field(..., ge=0)
    n_mrt_within_1km: int = Field(..., ge=0)
    dist_to_nearest_bus_stop_m: float = Field(..., ge=0)
    n_bus_stop_within_1km: int = Field(..., ge=0)
    month_index: int = Field(..., ge=0)
    dist_to_food_m: float = Field(..., ge=0)
    n_food_within_1km: int = Field(..., ge=0)
    dist_to_supermarket_m: float = Field(..., ge=0)
    n_supermarket_within_1km: int = Field(..., ge=0)


class PredictResponse(BaseModel):
    predicted_price: float
    is_dummy: bool
