from fastapi import APIRouter, Query, Depends
from services.assembly.assembly_api import AccessAssemblyLineApi
from helpers.services.assembly import BuildPipeline, DependeciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from database.queries import UpsertInfos
from helpers.log.logger import logger
import polars as pl


router = APIRouter()
log = logger("assembly")


@router.get("/response/json", summary="Get Response From Assembly Line API")
def get_json_response(api: AccessAssemblyLineApi = Depends(DependeciesInjection.get_api)):
    log.info("Rota /response/json chamada — iniciando coleta de JSON da Assembly Line API")

    try:
        response = api.get_json_response()
        log.info("JSON obtido com sucesso da Assembly Line API")
        return response

    except Exception as e:
        log.error("Erro ao obter JSON da Assembly Line API", exc_info=True)
        raise HTTP_Exceptions().http_502("Erro ao buscar origem: ", e)


@router.get("/response/processed", summary="Get Processed Response From Assembly Line API")
def get_processed_response(
    api: AccessAssemblyLineApi = Depends(DependeciesInjection.get_api),
    limit: int = Query(50, ge=1, le=100000)
):
    log.info(f"Rota /response/processed chamada — limit={limit}")

    try:
        df = BuildPipeline().build_assembly(api)
        log.info(f"Processamento concluído — total de registros: {df.height}")
        return df.head(limit).to_dicts()

    except Exception as e:
        log.error("Erro ao processar registros da Assembly Line API", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro ao processar registros:", e)


@router.post("/upsert", summary="Upsert Assembly Line Values In The DataBase")
def upsert_assembly(
    api: AccessAssemblyLineApi = Depends(DependeciesInjection.get_api),
    upsert: UpsertInfos = Depends(DependeciesInjection.get_upsert),
    batch_size: int = Query(10000, ge=1, le=100000)
):
    log.info(f"Rota POST /upsert chamada — batch_size={batch_size}")

    try:
        df = BuildPipeline().build_assembly(api)
        log.info(f"Pipeline executado com sucesso — registros processados: {df.select(pl.len()).collect().item()}")

        rows = upsert.upsert_df("assembly_line", df, batch_size)
        log.info(f"Upsert realizado com sucesso — linhas gravadas: {rows}")

        return {
            "message": "Upsert concluído com sucesso.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "assembly_line",
        }

    except Exception as e:
        log.error("Erro durante operação de upsert no banco", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro no upsert:", e)