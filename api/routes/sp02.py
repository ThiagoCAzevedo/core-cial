from fastapi import APIRouter, Depends
from helpers.services.sp02 import BuildPipeline, DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from services.requests_checker.sp02 import SP02_Session, SP02_Actions


router = APIRouter()


@router.get("/find-registry")
def sp02_find_lt22(
    svc: SP02_Session = Depends(DependenciesInjection.get_sp02_session)
):
    try:
        session = svc.sap.get_session()
        job = BuildPipeline.find_lt22(session)

        return {"found": job is not None, "job": job}

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro ao procurar job LT22", e)


@router.post("/open-registry")
def sp02_open(
    svc: SP02_Session = Depends(DependenciesInjection.get_sp02_session)
):
    try:
        svc.open()
        return {"message": "SP02 aberta com sucesso."}

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro ao abrir SP02", e)
    

@router.post("/download-registry")
def sp02_download(
    index: int,
    svc: SP02_Actions = Depends(DependenciesInjection.get_sp02_actions)
):
    try:
        session = svc.sap.get_session()
        svc.download(session, index)

        return {"message": "Download concluído."}

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro no download do SP02", e)
    

@router.delete("/clean-registry")
def sp02_clean(
    index: int,
    svc: SP02_Actions = Depends(DependenciesInjection.get_sp02_actions)
):
    try:
        svc.clean(index)
        return {"message": "LT22 apagado do SP02."}

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro ao limpar job do SP02", e)