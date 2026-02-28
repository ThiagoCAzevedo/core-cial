from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from modules.requests_checker.application.service_lt22 import LT22Service
from modules.sap_manager.application.session_manager import SAPSessionManager
from database.session import get_db
from common.http_errors import http_500, http_400
from common.logger import logger


router = APIRouter()
log = logger("requests_checker")


def get_lt22_service(
    db: Session = Depends(get_db),
    sap_mgr: SAPSessionManager = Depends(SAPSessionManager),
) -> LT22Service:
    log.info("Creating LT22Service")
    sap = sap_mgr.get_session()
    return LT22Service(sap, db)


@router.post("/lt22/open", summary="Open LT22 screen")
def lt22_open(
    svc: LT22Service = Depends(get_lt22_service),
):
    log.info("POST /requests-checker/lt22/open — opening LT22 session")
    try:
        if not svc.sap:
            raise http_400("No active SAP session", "Execute /sap/session first")
        
        svc.open_lt22()
        log.info("LT22 session opened successfully")
        return {"message": "LT22 opened successfully."}
    except Exception as e:
        log.error("Error opening LT22", exc_info=True)
        raise http_500("Error opening LT22: ", e)


@router.post("/lt22/request", summary="Execute LT22 pipeline")
def lt22_request(
    svc: LT22Service = Depends(get_lt22_service),
):
    log.info("POST /requests-checker/lt22/request — executing LT22 request pipeline")
    try:
        if not svc.sap:
            raise http_400("No active SAP session", "Execute /sap/session first")
        
        result = svc.request_lt22()
        log.info("LT22 request pipeline executed successfully")
        return {"message": "LT22 executed successfully.", "success": result}
    except Exception as e:
        log.error("Error in LT22 request", exc_info=True)
        raise http_500("Error in LT22 request: ", e)
