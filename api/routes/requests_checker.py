from fastapi import APIRouter, Depends
from helpers.services.requests_checker import LT22_BuildPipeline, SP02_BuildPipeline, DependenciesInjection
from services.requests_checker.lt22 import LT22_Session
from services.requests_checker.sp02 import SP02_Session, SP02_Actions
from helpers.services.http_exception import HTTP_Exceptions
from helpers.log.logger import logger
from services.sap_manager.session_manager import SAPSessionManager

router = APIRouter()
log = logger("requests_checker")


@router.post("/lt22/request", summary="Execute LT22 pipeline")
def lt22_request(
    svc: SAPSessionManager = Depends(DependenciesInjection.get_sap_session)
):
    log.info("POST /requests-checker/lt22/request — starting LT22 execution")

    try:
        session = svc.get_session()
        log.info("LT22 session opened successfully — starting pipeline")

        LT22_BuildPipeline.request(session)
        log.info("LT22 pipeline executed successfully")

        return {"message": "LT22 executed successfully."}

    except Exception as e:
        log.error("Error executing complete LT22 process", exc_info=True)
        raise HTTP_Exceptions().http_500("Error executing LT22: ", e)


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


@router.get("/sp02/find-registry", summary="Find LT22 registry inside SP02")
def sp02_find_lt22(
    svc: SP02_Session = Depends(DependenciesInjection.get_sp02_session)
):
    log.info("GET /requests-checker/sp02/find-registry — searching LT22 in SP02")

    try:
        session = svc.sap.get_session()
        log.info("SAP session retrieved from SessionManager")

        job = SP02_BuildPipeline.find_lt22(session)

        log.info(f"Search completed — found={job is not None}")
        return {"found": job is not None, "job": job}

    except Exception as e:
        log.error("Error searching for LT22 job in SP02", exc_info=True)
        raise HTTP_Exceptions().http_500("Error searching for LT22 job in SP02", e)


@router.post("/sp02/open-registry", summary="Open SP02 screen")
def sp02_open(
    svc: SP02_Session = Depends(DependenciesInjection.get_sp02_session)
):
    log.info("POST /requests-checker/sp02/open-registry — opening SP02 screen")

    try:
        svc.open()
        log.info("SP02 opened successfully")

        return {"message": "SP02 opened successfully."}

    except Exception as e:
        log.error("Error opening SP02", exc_info=True)
        raise HTTP_Exceptions().http_500("Error opening SP02: ", e)


@router.post("/sp02/download-registry", summary="Download output from SP02 registry")
def sp02_download(
    index: int,
    svc: SP02_Actions = Depends(DependenciesInjection.get_sp02_actions)
):
    log.info(f"POST /requests-checker/sp02/download-registry — index={index}")

    try:
        session = svc.sap.get_session()
        log.info("SAP session retrieved from SessionManager")

        svc.download(session, index)
        log.info(f"SP02 download completed — index={index}")

        return {"message": "Download completed."}

    except Exception as e:
        log.error("Error during SP02 download", exc_info=True)
        raise HTTP_Exceptions().http_500("Error during SP02 download: ", e)


@router.delete("/sp02/clean-registry", summary="Delete LT22 registry from SP02")
def sp02_clean(
    index: int,
    svc: SP02_Actions = Depends(DependenciesInjection.get_sp02_actions)
):
    log.info(f"DELETE /requests-checker/sp02/clean-registry — index={index}")

    try:
        svc.clean(index)
        log.info(f"LT22 job removed successfully — index={index}")

        return {"message": "LT22 deleted from SP02."}

    except Exception as e:
        log.error("Error deleting job from SP02", exc_info=True)
        raise HTTP_Exceptions().http_500("Error deleting job from SP02: ", e)