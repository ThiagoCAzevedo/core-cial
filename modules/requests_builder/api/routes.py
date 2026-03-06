from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from modules.requests_builder.application.service_to_request import QuantityToRequestService
from modules.requests_builder.application.service_lm01 import LM01RequesterService
from modules.sap_manager.application.session_manager import SAPSessionManager
from modules.requests_builder.infrastructure.repositories import SQLAlchemyRequestsRepository
from database.session import get_db
from common.http_errors import http_500, http_400
from common.logger import logger


router = APIRouter()
log = logger("requests_builder")


def get_to_request_service(db: Session = Depends(get_db)):
    return QuantityToRequestService(db)


def get_lm01_requester(
    db: Session = Depends(get_db),
    sap_mgr: SAPSessionManager = Depends(SAPSessionManager),
) -> LM01RequesterService:
    log.info("Creating LM01RequesterService")
    sap = sap_mgr.get_session()
    return LM01RequesterService(sap, db)


@router.get("/response/to-request", summary="Get values to request")
def get_to_request(
    svc: QuantityToRequestService = Depends(get_to_request_service),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"GET /requests-builder/response/to-request — limit={limit}")
    try:
        df = svc.define_diference_to_request().collect()
        log.info(f"Values to request successfully loaded — total rows: {df.height}")
        return df.head(limit).to_dicts()
    except Exception as e:
        log.error("Error fetching values to request", exc_info=True)
        raise http_500("Error fetching values to request: ", e)
    

@router.get("/response/requests-made/db", summary="Get values requested")
def get_requests_made(db: Session = Depends(get_db)):
    try:
        repo = SQLAlchemyRequestsRepository(db)
        records = repo.get_all_requests()
        return records

    except Exception as e:
        log.error(f"Failed to fetch from database: {str(e)}", exc_info=True)
        raise http_500("Error fetching requested values: ", e)


@router.post("/upsert/to-request", summary="Upsert 'to request' values into the database")
def upsert_to_request(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    svc: QuantityToRequestService = Depends(get_to_request_service),
):
    log.info(f"POST /requests-builder/upsert/to-request — batch_size={batch_size}")
    try:
        rows = svc.upsert_to_request(batch_size)
        log.info(f"Upsert completed — rows inserted: {rows}")
        return {
            "message": "Upsert 'to request' completed successfully.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "requests_made",
        }
    except Exception as e:
        log.error("Error during upsert (to request)", exc_info=True)
        raise http_500("Error during upsert (to request): ", e)


@router.post("/requester", summary="Send 'to request' values to SAP LM01")
def requester(
    svc: LM01RequesterService = Depends(get_lm01_requester),
):
    log.info("POST /requests-builder/requester — starting SAP requester execution")
    try:
        if not svc.sap:
            log.error("No active SAP session found")
            raise http_400(
                "No active SAP session.",
                "Before using this endpoint, execute /sap/session to open a SAP session."
            )

        rows = svc.request_lm01()
        log.info(f"SAP requester executed — total rows processed: {rows}")
        return {
            "message": "Requester completed successfully.",
            "rows": rows,
        }
    except Exception as e:
        log.error("Error in requester (to request)", exc_info=True)
        raise http_500("Error in requester (to request): ", e)
