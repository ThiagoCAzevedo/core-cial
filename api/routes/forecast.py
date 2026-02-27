from fastapi import APIRouter, Query, Depends
from services.forecast.buff_al import ReturnBuffAssemblyLineValues
from services.forecast.fx4pd import ReturnFX4PDValues
from services.forecast.forecaster import DefineForecastValues
from helpers.services.forecast import BuildPipeline, DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from database.queries import UpsertInfos
from helpers.log.logger import logger
import polars as pl


router = APIRouter()
log = logger("forecast")


# -- GET VALUES FROM LANE BUFFER --
@router.get("/response/buffer-al", summary="Get buffer values from Assembly Line database")
def get_buffer_al_response(
    svc: ReturnBuffAssemblyLineValues = Depends(DependenciesInjection.get_buff_al_service),
    limit: int = Query(100, ge=1, le=100000),
):
    log.info(f"GET /forecast/response/buffer-al — limit={limit}")

    try:
        df = svc.return_values_from_db().collect()
        log.info(f"Successfully obtained Buffer Assembly Line values — rows: {df.height}")
        return df.head(limit).to_dicts()

    except Exception as e:
        log.error("Error obtaining buffer values from Assembly Line database", exc_info=True)
        raise HTTP_Exceptions().http_502("Error obtaining buffer values from Assembly Line database: ", e)


# -- FX4PD ROUTES -- 
@router.get("/response/fx4pd", summary="Get values from FX4PD API")
def get_fx4pd_response(
    svc: ReturnFX4PDValues = Depends(DependenciesInjection.get_fx4pd_service),
    limit: int = Query(100, ge=1, le=100000),
):
    log.info(f"GET /forecast/response/fx4pd — limit={limit}")

    try:
        df = BuildPipeline().build_forecast(svc).collect()
        log.info(f"FX4PD successfully obtained — total rows: {df.height}")
        return df.head(limit).to_dicts()

    except Exception as e:
        log.error("Error fetching FX4PD source", exc_info=True)
        raise HTTP_Exceptions().http_502("Error fetching FX4PD source: ", e)


@router.post("/upsert/fx4pd", summary="Upsert FX4PD values into the database")
def upsert_fx4pd(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    fx4pd_svc: ReturnFX4PDValues = Depends(DependenciesInjection.get_fx4pd_service),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    log.info(f"POST /forecast/upsert/fx4pd — batch_size={batch_size}")

    try:
        lf = BuildPipeline().build_forecast(fx4pd_svc)
        rows = upsert_svc.upsert_df("fx4pd", lf, batch_size)

        log.info(f"FX4PD upsert completed — rows upserted: {rows}")

        return {
            "message": "Upsert completed successfully.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "fx4pd",
        }

    except Exception as e:
        log.error("Error during FX4PD upsert", exc_info=True)
        raise HTTP_Exceptions().http_500("Error during FX4PD upsert: ", e)


# -- ROUTES TO FINAL FORECAST --
@router.get("/result", summary="Get forecasted values")
def get_forecast_result(
    svc: DefineForecastValues = Depends(DependenciesInjection.get_forecast_service),
    limit: int = Query(100, ge=1, le=100000),
):
    log.info(f"GET /forecast/result — limit={limit}")

    try:
        df = svc.join_fx4pd_pkmc_pk05().collect()
        log.info(f"Forecast successfully obtained — total rows: {df.height}")
        return df.head(limit).to_dicts()

    except Exception as e:
        log.error("Error fetching forecast source", exc_info=True)
        raise HTTP_Exceptions().http_502("Error fetching forecast source: ", e)


@router.post("/upsert", summary="Upsert forecasted values into the database")
def upsert_forecast_pipeline(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    fx4pd_svc: ReturnFX4PDValues = Depends(DependenciesInjection.get_fx4pd_service),
    forecast_svc: DefineForecastValues = Depends(DependenciesInjection.get_forecast_service),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    log.info(f"POST /forecast/upsert — batch_size={batch_size}")

    try:
        # FX4PD
        lf_fx4pd = BuildPipeline().build_forecast(fx4pd_svc)
        total_fx4pd = lf_fx4pd.select(pl.len()).collect().item()
        log.info(f"FX4PD loaded — rows: {total_fx4pd}")
        upsert_svc.upsert_df("fx4pd", lf_fx4pd, batch_size)
        log.info("FX4PD upsert completed")  

        # FORECAST
        lf_forecast = forecast_svc.join_fx4pd_pkmc_pk05()
        print()
        rows_forecast = upsert_svc.upsert_df("forecast", lf_forecast, batch_size)
        log.info(f"Forecast upsert completed — rows: {rows_forecast}")

        return {
            "message": "Upsert completed successfully.",
            "rows": {
                "fx4pd": total_fx4pd,
                "forecast": rows_forecast,
            },
            "batch_size": batch_size,
            "tables": ["fx4pd", "forecast"],
        }

    except Exception as e:
        log.error("Error during forecast pipeline upsert", exc_info=True)
        raise HTTP_Exceptions().http_500("Error during forecast pipeline upsert: ", e)