from fastapi import APIRouter, Depends
from services.sap.client import SAP_Client
from services.request.requester import LM01_Requester
from services.sap.session_manager import SAPSessionManager
from helpers.services.sap import DependenciesInjection 
from helpers.services.http_exception import HTTP_Exceptions


router = APIRouter()


@router.post("/session", summary="Creates a SAP Session And Stores It")
def create_sap_session(
    client: SAP_Client = Depends(DependenciesInjection.get_sap_client),
    session_manager: SAPSessionManager = Depends(DependenciesInjection.get_sap_session)
):
    try:
        session = client.connect()
        session_manager.set_session(session)

        return {"message": "SAP session created successfully!"}

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro ao criar sessão SAP", e)
    

@router.post("/requester", summary="Requester To Request Values In SAP")
def requester(
    svc: LM01_Requester = Depends(DependenciesInjection.get_lm01_requester),
    sap = Depends(DependenciesInjection.get_sap_session)
):
    try:
        session = sap.get_session()

        if not session:
            raise HTTP_Exceptions().http_400(
                "Nenhuma sessão SAP ativa.",
                "Antes de usar este endpoint, execute /sap/session para abrir uma sessão SAP."
            )

        rows = svc._request_lm01()

        return {
            "message": "Requester concluído com sucesso.",
            "rows": rows,
        }

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro no request (to request)", e)