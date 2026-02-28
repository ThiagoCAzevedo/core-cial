from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from modules.requests_closure.application.service_lt22_process import LT22ProcessService
from database.session import get_db
from common.http_errors import http_500
from common.logger import logger


router = APIRouter()
log = logger("requests_closure")


def get_lt22_process_service(db: Session = Depends(get_db)) -> LT22ProcessService:
    return LT22ProcessService(db)


@router.get("/response/processed/lt22", summary="Get cleaned LT22 values")
def get_processed_lt22(
    svc: LT22ProcessService = Depends(get_lt22_process_service),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"GET /requests-closure/response/processed/lt22 — limit={limit}")
    try:
        df = svc.process_lt22_pipeline().head(limit).collect()
        log.info(f"LT22 processed successfully — rows returned: {df.height}")
        return df.to_dicts()
    except Exception as e:
        log.error("Error processing LT22 (clean)", exc_info=True)
        raise http_500("Error processing LT22 (clean): ", e)


@router.post("/update-delete", summary="Update lb_balance and delete RequestsMade entries")
def update_and_delete_lt22(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    svc: LT22ProcessService = Depends(get_lt22_process_service),
):
    log.info(f"POST /requests-closure/update-delete — batch_size={batch_size}")
    try:
        import polars as pl
        df = svc.process_lt22_pipeline()
        total_rows = df.select(pl.len()).collect().item()
        log.info(f"LT22 processed — total rows: {total_rows}")

        svc.update_lb_balance(df, batch_size)
        log.info("lb_balance update executed successfully")

        svc.delete_requests_made(df, batch_size)
        log.info("Requests made cleanup completed")

        return {
            "message": "Update and delete completed successfully.",
            "rows_processed": total_rows,
            "batch_size": batch_size,
        }
    except Exception as e:
        log.error("Error in update/delete operation", exc_info=True)
        raise http_500("Error in update/delete operation: ", e)
