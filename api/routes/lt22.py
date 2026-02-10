from fastapi import APIRouter, Depends
from helpers.services.lt22 import LT22_Session, BuildPipeline, DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from helpers.log.logger import logger


router = APIRouter()
log = logger("sap")


@router.post("/request")
def lt22_request(
    svc: LT22_Session = Depends(DependenciesInjection.get_lt22_session)
):
    log.info("Rota POST /request chamada — iniciando execução LT22")

    try:
        session = svc.open()
        log.info("Sessão LT22 aberta com sucesso — iniciando pipeline")
        
        BuildPipeline.request(session)
        log.info("Pipeline LT22 executado com sucesso")

        return {"message": "LT22 executado com sucesso."}

    except Exception as e:
        log.error("Erro ao executar processo completo de LT22", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro ao executar LT22", e)



@router.post("/open")
def lt22_open(
    svc: LT22_Session = Depends(DependenciesInjection.get_lt22_session)
):
    log.info("Rota POST /open chamada — abrindo sessão LT22")

    try:
        svc.open()
        log.info("Sessão LT22 aberta com sucesso")

        return {"message": "LT22 aberta com sucesso."}

    except Exception as e:
        log.error("Erro ao abrir LT22", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro ao abrir LT22", e)