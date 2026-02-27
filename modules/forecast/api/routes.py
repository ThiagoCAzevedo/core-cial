from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
import polars as pl
from modules.forecast.application.pipeline import ForecastPipeline
from modules.forecast.application.service_fx4pd import FX4PDService
from modules.forecast.application.service_buf_al import BuffALService
from modules.forecast.application.service_forecast import ForecastService
from modules.forecast.infrastructure.repository import ForecastRepository
from common.http_errors import http_500
from common.logger import logger
from database.session import get_db


router = APIRouter()
log = logger("forecast")


def get_fx4pd_service():
    return FX4PDService()

def get_buff_al_service(db: Session = Depends(get_db)):
    return BuffALService(db)

def get_forecast_service(db: Session = Depends(get_db)):
    return ForecastService(db)

def get_repo(db: Session = Depends(get_db)):
    return ForecastRepository(db)


@router.get("/response/buffer-al")
def get_buffer_al(
    svc: BuffALService = Depends(get_buff_al_service),
    limit: int = Query(100, ge=1)
):
    try:
        df = svc.return_values_from_db().collect()
        return df.head(limit).to_dicts()
    except Exception as e:
        raise http_500("Error getting buffer AL values: ", e)


@router.get("/response/fx4pd")
def get_fx4pd(
    svc: FX4PDService = Depends(get_fx4pd_service),
    limit: int = Query(100, ge=1),
):
    try:
        lf = ForecastPipeline().build_fx4pd(svc)
        df = lf.collect()
        return df.head(limit).to_dicts()
    except Exception as e:
        raise http_500("Error getting FX4PD values: ", e)


@router.post("/upsert/fx4pd")
def upsert_fx4pd(
    batch_size: int = Query(10_000, ge=1),
    fx4pd: FX4PDService = Depends(get_fx4pd_service),
    repo: ForecastRepository = Depends(get_repo),
):
    try:
        lf = ForecastPipeline().build_fx4pd(fx4pd)
        rows = repo.upsert_df("fx4pd", lf, batch_size)
        return {"rows": rows, "batch_size": batch_size}
    except Exception as e:
        raise http_500("Error during FX4PD upsert: ", e)


@router.get("/result")
def get_result(
    svc: ForecastService = Depends(get_forecast_service),
    limit: int = Query(100, ge=1)
):
    try:
        df = svc.join_fx4pd_pkmc_pk05().collect()
        return df.head(limit).to_dicts()
    except Exception as e:
        raise http_500("Error getting forecast result: ", e)


@router.post("/upsert")
def upsert_pipeline(
    batch_size: int = Query(10_000, ge=1),
    fx4pd_svc: FX4PDService = Depends(get_fx4pd_service),
    forecast_svc: ForecastService = Depends(get_forecast_service),
    repo: ForecastRepository = Depends(get_repo),
):
    try:
        # FX4PD
        lf_fx4pd = ForecastPipeline().build_fx4pd(fx4pd_svc)
        total_fx4pd = lf_fx4pd.select(pl.len()).collect().item()
        repo.upsert_df("fx4pd", lf_fx4pd, batch_size)

        # FORECAST
        lf_forecast = forecast_svc.join_fx4pd_pkmc_pk05()
        total_forecast = repo.upsert_df("forecast", lf_forecast, batch_size)

        return {
            "rows": {
                "fx4pd": total_fx4pd,
                "forecast": total_forecast
            },
            "batch_size": batch_size,
        }

    except Exception as e:
        raise http_500("Error in final forecast upsert: ", e)