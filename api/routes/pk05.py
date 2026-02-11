from fastapi import APIRouter, Query, Depends
from services.static.pk05 import PK05_Cleaner, PK05_DefineDataframe
from helpers.services.static import PK05_BuildPipeline, DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from database.queries import UpsertInfos
from helpers.log.logger import logger
import polars as pl


router = APIRouter()
log = logger("static")


@router.get("/response/raw", summary="Get raw PK05 values")
def get_raw_pk05(
    svc: PK05_DefineDataframe = Depends(DependenciesInjection.get_pk05),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"GET /static/response/raw — limit={limit}")

    try:
        df = svc.create_df().collect()
        log.info(f"Raw PK05 successfully loaded — total rows: {df.height}")
        return df.head(limit).to_dicts()

    except Exception as e:
        log.error("Error fetching raw PK05", exc_info=True)
        raise HTTP_Exceptions().http_502("Error fetching raw PK05: ", e)


@router.get("/response/processed", summary="Get cleaned PK05 values")
def get_clean_pk05(
    raw_svc: PK05_DefineDataframe = Depends(DependenciesInjection.get_pk05),
    cleaner_svc: PK05_Cleaner = Depends(DependenciesInjection.get_pk05_cleaner),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"GET /static/response/processed — limit={limit}")

    try:
        df = PK05_BuildPipeline.build_pk05(raw_svc, cleaner_svc).head(limit).collect()
        log.info(f"Cleaned PK05 successfully processed — rows returned: {df.height}")
        return df.to_dicts()

    except Exception as e:
        log.error("Error processing PK05 (clean)", exc_info=True)
        raise HTTP_Exceptions().http_502("Error processing PK05 (clean): ", e)


@router.post("/upsert", summary="Upsert cleaned PK05 values into the database")
def upsert_pk05(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    raw_svc: PK05_DefineDataframe = Depends(DependenciesInjection.get_pk05),
    cleaner_svc: PK05_Cleaner = Depends(DependenciesInjection.get_pk05_cleaner),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    log.info(f"POST /static/upsert — batch_size={batch_size}")

    try:
        df = PK05_BuildPipeline.build_pk05(raw_svc, cleaner_svc)
        total_rows = df.select(pl.len()).collect().item()

        log.info(f"PK05 processed before upsert — total rows: {total_rows}")

        rows = upsert_svc.upsert_df("pk05", df, batch_size)

        log.info(f"PK05 upsert completed successfully — rows upserted: {rows}")

        return {
            "message": "PK05 upsert completed successfully.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "pk05",
        }

    except Exception as e:
        log.error("Error during PK05 upsert", exc_info=True)
        raise HTTP_Exceptions().http_500("Error during PK05 upsert: ", e)