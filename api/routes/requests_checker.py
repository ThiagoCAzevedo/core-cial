from fastapi import APIRouter, Depends
from helpers.services.requests_checker import LT22_BuildPipeline, DependenciesInjection
from services.requests_checker.lt22 import LT22_Session
from helpers.services.http_exception import HTTP_Exceptions
from helpers.log.logger import logger
from database.database import get_db
from sqlalchemy.orm import Session


router = APIRouter()
log = logger("requests_checker")


@router.post("/lt22/open", summary="Open LT22 screen")
def lt22_open(
    svc: LT22_Session = Depends(DependenciesInjection.get_lt22_session)
):
    log.info("POST /requests-checker/lt22/open — opening LT22 session")

    try:
        svc.open()
        log.info("LT22 session opened successfully")

        return {"message": "LT22 opened successfully."}

    except Exception as e:
        log.error("Error opening LT22", exc_info=True)
        raise HTTP_Exceptions().http_500("Error opening LT22: ", e)


@router.post("/lt22/request")
def lt22_request(
    svc: LT22_Session = Depends(DependenciesInjection.get_lt22_session),
    db: Session = Depends(get_db)
):
    session = svc.open()
    LT22_BuildPipeline(db).request(session)
    return {"message": "LT22 executed successfully."}