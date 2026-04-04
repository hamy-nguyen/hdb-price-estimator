from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    # Core
    flat_type: int = Field(..., ge=1, le=8)
    floor_area_sqm: float = Field(..., gt=0, le=500)
    flat_model: int = Field(..., ge=0)
    storey_mid: float = Field(..., ge=1)
    month: int = Field(..., ge=1, le=12)
    year: int = Field(..., ge=1990, le=2100)
    quarter: int = Field(..., ge=1, le=4)
    month_index: int = Field(..., ge=0)

    # Building
    max_floor_lvl: float = Field(..., ge=1)
    total_dwelling_units: float = Field(..., ge=0)
    has_market_hawker: bool
    has_multistorey_carpark: bool
    year_completed: float = Field(..., ge=1960, le=2100)
    building_age: float = Field(..., ge=0)

    # Lease
    remaining_lease_years: float = Field(..., ge=0, le=99)
    lease_age: float = Field(..., ge=0)
    lease_age_sq: float = Field(..., ge=0)

    # Location
    latitude: float = Field(..., ge=1.1, le=1.5)
    longitude: float = Field(..., ge=103.5, le=104.1)
    planning_area: int = Field(..., ge=0)
    region: int = Field(..., ge=0)
    town: int = Field(..., ge=0)

    # MRT
    dist_to_nearest_mrt_m: float = Field(..., ge=0)
    n_mrt_within_1km: int = Field(..., ge=0)

    # POI
    dist_to_school_m: float = Field(..., ge=0)
    n_school_within_1km: int = Field(..., ge=0)
    dist_to_mall_m: float = Field(..., ge=0)
    n_mall_within_1km: int = Field(..., ge=0)
    dist_to_food_m: float = Field(..., ge=0)
    n_food_within_1km: int = Field(..., ge=0)
    dist_to_park_m: float = Field(..., ge=0)
    n_park_within_1km: int = Field(..., ge=0)
    dist_to_supermarket_m: float = Field(..., ge=0)
    n_supermarket_within_1km: int = Field(..., ge=0)

    # Carpark
    dist_to_nearest_carpark_m: float = Field(..., ge=0)
    n_carparks_within_500m: int = Field(..., ge=0)
    gantry_height: float = Field(..., ge=0)
    car_park_decks: float = Field(..., ge=0)
    has_free_parking: bool
    has_short_term_parking: bool
    has_night_parking: bool
    has_car_park_basement: bool

    # Bus
    dist_to_nearest_bus_stop_m: float = Field(..., ge=0)
    n_bus_stop_within_1km: int = Field(..., ge=0)
    nearest_bus_stop_operating_days_per_week: float = Field(..., ge=0, le=7)
    nearest_bus_stop_busyness_level: int = Field(..., ge=0)

    # Tourist
    dist_to_nearest_tourist_attraction_m: float = Field(..., ge=0)

    # OneMap
    transport_school_pct_bus: float = Field(..., ge=0, le=1)
    transport_school_pct_mrt: float = Field(..., ge=0, le=1)
    transport_school_pct_mrt_bus: float = Field(..., ge=0, le=1)
    transport_school_pct_car: float = Field(..., ge=0, le=1)
    tenancy_pct_owner: float = Field(..., ge=0, le=1)
    dwelling_pct_1room: float = Field(..., ge=0, le=1)
    dwelling_pct_2room: float = Field(..., ge=0, le=1)
    dwelling_pct_3room: float = Field(..., ge=0, le=1)
    dwelling_pct_4room: float = Field(..., ge=0, le=1)
    dwelling_pct_5room: float = Field(..., ge=0, le=1)
    dwelling_pct_exec: float = Field(..., ge=0, le=1)
    dwelling_pct_studio: float = Field(..., ge=0, le=1)
    dwelling_pct_multi_gen: float = Field(..., ge=0, le=1)

    # Interactions
    floor_area_x_storey: float = Field(..., ge=0)
    storey_ratio: float = Field(..., ge=0)
    town_price_trend_6m: float


class PredictResponse(BaseModel):
    predicted_price: float
    is_dummy: bool
