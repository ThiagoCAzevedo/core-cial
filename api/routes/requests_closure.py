from fastapi import APIRouter, Query, Depends
from services.requests_closure.processor import DefineDataFrame, CleanDataFrame
from services.requests_closure.update import UpdateDeleteValues
from helpers.services.requests_closure import DependenciesInjection, LT22_BuildPipeline
from helpers.services.http_exception import HTTP_Exceptions
from helpers.log.logger import logger
import polars as pl


router = APIRouter()
log = logger("requests_closure")


@router.get("/response/processed/lt22", summary="Get cleaned LT22 values")
def get_processed_lt22(
    raw_svc: DefineDataFrame = Depends(DependenciesInjection.get_lt22),
    cleaner_svc: CleanDataFrame = Depends(DependenciesInjection.get_lt22_cleaner),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"GET /requests-closure/response/processed/lt22 — limit={limit}")

    try:
        df = LT22_BuildPipeline.build_lt22(raw_svc, cleaner_svc).head(limit).collect()
        log.info(f"LT22 processed successfully — rows returned: {df.height}")
        return df.to_dicts()

    except Exception as e:
        log.error("Error processing LT22 (clean)", exc_info=True)
        raise HTTP_Exceptions().http_502("Error processing LT22 (clean): ", e)


@router.post("/update-delete", summary="Update lb_balance and delete RequestsMade entries")
def update_and_delete_lt22(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    raw_svc: DefineDataFrame = Depends(DependenciesInjection.get_lt22),
    cleaner_svc: CleanDataFrame = Depends(DependenciesInjection.get_lt22_cleaner),
    update_delete_svc: UpdateDeleteValues = Depends(DependenciesInjection.get_upsert_service),
):
    log.info(f"POST /requests-closure/update-delete — batch_size={batch_size}")

    try:
        df = LT22_BuildPipeline.build_lt22(raw_svc, cleaner_svc)
        total_rows = df.select(pl.len()).collect().item()

        log.info(f"LT22 processed — total rows: {total_rows}")

        update_delete_svc.update_lb_balance(df)
        log.info("lb_balance update executed successfully")

        update_delete_svc.delete_requests_made(df)
        log.info("RequestsMade deletion executed successfully")

        return {
            "message": "Update and delete operations executed successfully.",
            "table_update": "pkmc",
            "table_delete": "requests_made",
            "batch_size": batch_size,
        }

    except Exception as e:
        log.error("Error executing LT22 update/delete route", exc_info=True)
        raise HTTP_Exceptions().http_500("Error executing LT22 update/delete: ", e)