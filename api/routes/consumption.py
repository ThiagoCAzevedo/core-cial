from fastapi import APIRouter, Query, Depends
from services.consumption.consumer import ConsumeValues
from helpers.services.consumption import DependeciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from helpers.log.logger import logger


router = APIRouter()
log = logger("consumption")


@router.get("/response/to-consume", summary="Get Values To Consume")
def get_to_consume_response(svc: ConsumeValues = Depends(DependeciesInjection.get_consume)):
    log.info("Rota /response/to-consume chamada — iniciando coleta de valores a consumir")

    try:
        data = svc.get_raw_response()
        log.info("Valores a consumir obtidos com sucesso")
        return data

    except Exception as e:
        log.error("Erro ao buscar valores a consumir", exc_info=True)
        raise HTTP_Exceptions().http_502("Erro ao buscar origem: ", e)



@router.put("/update/to-consume", summary="Update Values To Consume")
def update_to_consume(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    svc: ConsumeValues = Depends(DependeciesInjection.get_consume),
):
    log.info(f"Rota /update/to-consume chamada — batch_size={batch_size}")

    try:
        df = svc.values_to_consume()
        log.info(f"Valores de consumo carregados — total de linhas: {df.height}")

        updated_rows = svc._update_infos(df=df, batch_size=batch_size)
        log.info(f"Update executado com sucesso — linhas atualizadas: {updated_rows}")

        return {
            "message": "Update executado com sucesso.",
            "rows_updated": updated_rows,
            "batch_size": batch_size,
        }

    except Exception as e:
        log.error("Erro ao atualizar valores (to-consume)", exc_info=True)
        raise HTTP_Exceptions().http_502("Erro ao atualizar valores (to-consume): ", e)