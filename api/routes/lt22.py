from fastapi import APIRouter, Depends
from helpers.services.lt22 import LT22_Session, BuildPipeline, DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions


router = APIRouter()


@router.post("/request")
def lt22_request(
    svc: LT22_Session = Depends(DependenciesInjection.get_lt22_session)
):
    try:
        session = svc.open()
        BuildPipeline.request(session)

        return {"message": "LT22 executado com sucesso."}

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro ao executar LT22", e)


@router.post("/open")
def lt22_open(
    svc: LT22_Session = Depends(DependenciesInjection.get_lt22_session)
):
    try:
        svc.open()
        return {"message": "LT22 aberta com sucesso."}

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro ao abrir LT22", e)