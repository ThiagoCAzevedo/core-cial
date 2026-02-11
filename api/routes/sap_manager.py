from fastapi import APIRouter, Depends
from services.sap_manager.client import SAP_Client
from services.sap_manager.session_manager import SAPSessionManager
from helpers.services.sap_manager import DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from helpers.log.logger import logger


router = APIRouter()
log = logger("sap_manager")


@router.post("/session", summary="Create a SAP session and store it")
def create_sap_session(
    client: SAP_Client = Depends(DependenciesInjection.get_sap_client),
    session_manager: SAPSessionManager = Depends(DependenciesInjection.get_sap_session)
):
    log.info("POST /sap-manager/session — starting SAP session creation")

    try:
        session = client.connect()
        log.info("SAP session successfully connected")

        session_manager.set_session(session)
        log.info("SAP session stored in SessionManager")

        return {"message": "SAP session created successfully!"}

    except Exception as e:
        log.error("Error creating SAP session", exc_info=True)
        raise HTTP_Exceptions().http_500("Error creating SAP session: ", e)