from fastapi import APIRouter, Query, Depends
from services.requests_builder.requester import QuantityToRequest, LM01_Requester
from helpers.services.requests_builder import DependenciesInjection, BuildPipeline
from helpers.services.http_exception import HTTP_Exceptions
from database.queries import UpsertInfos
from helpers.log.logger import logger
import polars as pl


router = APIRouter()
log = logger("requests_builder")


@router.get("/response/to-request", summary="Get Values To Request")
def get_to_request(
    svc: QuantityToRequest = Depends(DependenciesInjection.get_to_request),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"Rota /response/to-request chamada — limit={limit}")

    try:
        df = svc._define_diference_to_request().collect()
        log.info(f"Valores para requisição carregados — total de registros: {df.height}")
        return df.head(limit).to_dicts()

    except Exception as e:
        log.error("Erro ao buscar valores para solicitar", exc_info=True)
        raise HTTP_Exceptions().http_502("Erro ao buscar valores para solicitar: ", e)



@router.post("/upsert/to-request", summary="Upsert To Request Values In The DataBase")
def upsert_to_request(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    svc: QuantityToRequest = Depends(DependenciesInjection.get_to_request),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    log.info(f"Rota /upsert/to-request chamada — batch_size={batch_size}")

    try:
        df = BuildPipeline.build_to_request(svc)
        log.info(f"Valores processados — total de registros: {len(df)}")

        rows = upsert_svc.upsert_df("requests_made", df, batch_size)
        log.info(f"Upsert concluído — linhas gravadas: {rows}")

        return {
            "message": "Upsert To Request concluído com sucesso.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "requests_made",
        }

    except Exception as e:
        log.error("Erro no upsert (to request)", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro no upsert (to request)", e)
    

@router.post("/requester", summary="Requester To Request Values In SAP")
def requester(
    svc: LM01_Requester = Depends(DependenciesInjection.get_lm01_requester),
    sap = Depends(DependenciesInjection.get_sap_session)
):
    log.info("Rota POST /requester chamada — iniciando execução do requester SAP")

    try:
        session = sap.get_session()
        log.info("Sessão SAP recuperada do SessionManager")

        if not session:
            log.error("Nenhuma sessão SAP ativa encontrada")
            raise HTTP_Exceptions().http_400(
                "Nenhuma sessão SAP ativa.",
                "Antes de usar este endpoint, execute /sap/session para abrir uma sessão SAP."
            )

        rows = svc._request_lm01()
        log.info(f"Requester SAP executado — total de linhas processadas: {rows}")

        return {
            "message": "Requester concluído com sucesso.",
            "rows": rows,
        }

    except Exception as e:
        log.error("Erro no request (to request)", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro no request (to request)", e)