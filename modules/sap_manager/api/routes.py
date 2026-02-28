from fastapi import APIRouter, Depends
from modules.sap_manager.application.session_manager import SAPSessionManager
from modules.sap_manager.application.sap_client import SAP_Client
from common.http_errors import http_500
from common.logger import logger


router = APIRouter()
log = logger("sap_manager")


def get_sap_client():
    return SAP_Client()


def get_session_manager():
    return SAPSessionManager()


@router.post("/session", summary="Create a SAP session and store it")
def create_sap_session(
    client: SAP_Client = Depends(get_sap_client),
    session_manager: SAPSessionManager = Depends(get_session_manager),
):
    log.info("POST /sap-manager/session — starting SAP session creation")
    try:
        client.connect()
        log.info("SAP session successfully connected")

        session_manager.set_session(client)
        log.info("SAP session stored in SessionManager")

        return {"message": "SAP session created successfully!"}

    except Exception as e:
        log.error("Error creating SAP session", exc_info=True)
        raise http_500("Error creating SAP session: ", e)


@router.get("/status", summary="Get SAP session status")
def sap_status():
    log.info("GET /sap-manager/status — checking SAP session status")
    try:
        sess = SAPSessionManager.get_session()
        return {
            "session": repr(sess),
            "type": str(type(sess)),
            "has_run_transaction": hasattr(sess, "run_transaction") if sess else False
        }
    except Exception as e:
        log.error("Error getting SAP status", exc_info=True)
        raise http_500("Error getting SAP status: ", e)
