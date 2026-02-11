from fastapi import APIRouter, Depends
from services.sap_manager.client import SAP_Client
from services.sap_manager.session_manager import SAPSessionManager
from helpers.services.sap_manager import DependenciesInjection 
from helpers.services.http_exception import HTTP_Exceptions
from helpers.log.logger import logger


router = APIRouter()
log = logger("sap_manager")


@router.post("/session", summary="Creates a SAP Session And Stores It")
def create_sap_session(
    client: SAP_Client = Depends(DependenciesInjection.get_sap_client),
    session_manager: SAPSessionManager = Depends(DependenciesInjection.get_sap_session)
):
    log.info("Rota POST /session chamada — iniciando criação de sessão SAP")

    try:
        session = client.connect()
        log.info("Sessão SAP conectada com sucesso")

        session_manager.set_session(session)
        log.info("Sessão SAP armazenada no SessionManager")

        return {"message": "SAP session created successfully!"}

    except Exception as e:
        log.error("Erro ao criar sessão SAP", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro ao criar sessão SAP", e)