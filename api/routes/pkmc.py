from fastapi import APIRouter, Query, Depends
from services.static.pkmc import PKMC_DefineDataframe, PKMC_Cleaner
from helpers.services.static import PKMC_BuildPipeline, DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from database.queries import UpsertInfos
from helpers.log.logger import logger
import polars as pl


router = APIRouter()
log = logger("static")


@router.get("/response/raw", summary="Get raw PKMC values")
def get_raw_pkmc(
    svc: PKMC_DefineDataframe = Depends(DependenciesInjection.get_pkmc),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"GET /static/response/raw — limit={limit}")

    try:
        df = svc.create_df().collect()
        log.info(f"Raw PKMC successfully loaded — total rows: {df.height}")
        return df.head(limit).to_dicts()

    except Exception as e:
        log.error("Error fetching raw PKMC", exc_info=True)
        raise HTTP_Exceptions().http_502("Error fetching raw PKMC: ", e)


@router.get("/response/processed", summary="Get cleaned PKMC values")
def get_clean_pkmc(
    raw_svc: PKMC_DefineDataframe = Depends(DependenciesInjection.get_pkmc),
    cleaner_svc: PKMC_Cleaner = Depends(DependenciesInjection.get_pkmc_cleaner),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"GET /static/response/processed — limit={limit}")

    try:
        df = PKMC_BuildPipeline.build_pkmc(raw_svc, cleaner_svc).head(limit).collect()
        log.info(f"Cleaned PKMC processed successfully — rows returned: {df.height}")
        return df.to_dicts()

    except Exception as e:
        log.error("Error processing PKMC (clean)", exc_info=True)
        raise HTTP_Exceptions().http_502("Error processing PKMC (clean): ", e)


@router.post("/upsert", summary="Upsert PKMC values into the database")
def upsert_pkmc(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    raw_svc: PKMC_DefineDataframe = Depends(DependenciesInjection.get_pkmc),
    cleaner_svc: PKMC_Cleaner = Depends(DependenciesInjection.get_pkmc_cleaner),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    log.info(f"POST /static/upsert — batch_size={batch_size}")

    try:
        df = PKMC_BuildPipeline.build_pkmc(raw_svc, cleaner_svc)
        total_rows = df.select(pl.len()).collect().item()

        log.info(f"PKMC processed before upsert — total rows: {total_rows}")

        rows = upsert_svc.upsert_df("pkmc", df, batch_size)

        log.info(f"PKMC upsert completed successfully — rows upserted: {rows}")

        return {
            "message": "PKMC upsert completed successfully.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "pkmc",
        }

    except Exception as e:
        log.error("Error during PKMC upsert", exc_info=True)
        raise HTTP_Exceptions().http_500("Error during PKMC upsert: ", e)