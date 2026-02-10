from fastapi import APIRouter, Depends
from services.requests_checker.sp02 import SP02_Session, SP02_Actions
from helpers.services.sp02 import BuildPipeline, DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from helpers.log.logger import logger


router = APIRouter()
log = logger("sap")


@router.get("/find-registry", summary="Find LT22 Registry Inside SP02")
def sp02_find_lt22(
    svc: SP02_Session = Depends(DependenciesInjection.get_sp02_session)
):
    log.info("Rota GET /find-registry chamada — buscando LT22 no SP02")

    try:
        session = svc.sap.get_session()
        log.info("Sessão SAP recuperada no SessionManager")

        job = BuildPipeline.find_lt22(session)

        log.info(f"Busca concluída — encontrado={job is not None}")
        return {"found": job is not None, "job": job}

    except Exception as e:
        log.error("Erro ao procurar job LT22 no SP02", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro ao procurar job LT22", e)



@router.post("/open-registry", summary="Open SP02 Registry")
def sp02_open(
    svc: SP02_Session = Depends(DependenciesInjection.get_sp02_session)
):
    log.info("Rota POST /open-registry chamada — abrindo tela SP02")

    try:
        svc.open()
        log.info("SP02 aberta com sucesso")
        return {"message": "SP02 aberta com sucesso."}

    except Exception as e:
        log.error("Erro ao abrir SP02", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro ao abrir SP02", e)
    


@router.post("/download-registry", summary="Download Output From SP02 Registry")
def sp02_download(
    index: int,
    svc: SP02_Actions = Depends(DependenciesInjection.get_sp02_actions)
):
    log.info(f"Rota POST /download-registry chamada — index={index}")

    try:
        session = svc.sap.get_session()
        log.info("Sessão SAP recuperada no SessionManager")

        svc.download(session, index)
        log.info(f"Download do SP02 concluído — index={index}")

        return {"message": "Download concluído."}

    except Exception as e:
        log.error("Erro no download do SP02", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro no download do SP02", e)
    


@router.delete("/clean-registry", summary="Delete LT22 Registry From SP02")
def sp02_clean(
    index: int,
    svc: SP02_Actions = Depends(DependenciesInjection.get_sp02_actions)
):
    log.info(f"Rota DELETE /clean-registry chamada — index={index}")

    try:
        svc.clean(index)
        log.info(f"Job LT22 removido com sucesso — index={index}")
        return {"message": "LT22 apagado do SP02."}

    except Exception as e:
        log.error("Erro ao limpar job do SP02", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro ao limpar job do SP02", e)