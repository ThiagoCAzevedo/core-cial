from fastapi import APIRouter, Query, Depends
from services.static.pk05 import PK05_Cleaner, PK05_DefineDataframe
from helpers.services.static import PK05_BuildPipeline, DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from database.queries import UpsertInfos
from helpers.log.logger import logger
import polars as pl


router = APIRouter()
log = logger("static")


@router.get("/response/raw", summary="Get Raw PK05 Values")
def get_raw_pk05(
    svc: PK05_DefineDataframe = Depends(DependenciesInjection.get_pk05),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"Rota /response/raw chamada — limit={limit}")

    try:
        df = svc.create_df().collect()
        log.info(f"PK05 bruto carregado com sucesso — total de registros: {df.height}")
        return df.head(limit).to_dicts()

    except Exception as e:
        log.error("Erro ao buscar PK05 bruto", exc_info=True)
        raise HTTP_Exceptions().http_502("Erro ao buscar PK05 bruto: ", e)


@router.get("/response/processed", summary="Get Cleaned PK05 Values")
def get_clean_pk05(
    raw_svc: PK05_DefineDataframe = Depends(DependenciesInjection.get_pk05),
    cleaner_svc: PK05_Cleaner = Depends(DependenciesInjection.get_pk05_cleaner),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"Rota /response/processed chamada — limit={limit}")

    try:
        df = PK05_BuildPipeline.build_pk05(raw_svc, cleaner_svc).head(limit).collect()
        log.info(f"PK05 processado com sucesso — total de registros exibidos: {df.height}")
        return df.to_dicts()

    except Exception as e:
        log.error("Erro ao processar PK05 (clean)", exc_info=True)
        raise HTTP_Exceptions().http_502("Erro ao processar PK05 (clean): ", e)


@router.post("/upsert", summary="Upsert PK05 Values In The DataBase")
def upsert_pk05(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    raw_svc: PK05_DefineDataframe = Depends(DependenciesInjection.get_pk05),
    cleaner_svc: PK05_Cleaner = Depends(DependenciesInjection.get_pk05_cleaner),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    log.info(f"Rota POST /upsert chamada — batch_size={batch_size}")

    try:
        df = PK05_BuildPipeline.build_pk05(raw_svc, cleaner_svc)
        log.info(f"PK05 processado antes do upsert — total de registros: {df.select(pl.len()).collect().item()}")

        rows = upsert_svc.upsert_df("pk05", df, batch_size)
        log.info(f"Upsert PK05 realizado com sucesso — linhas upsertadas: {rows}")

        return {
            "message": "Upsert PK05 concluído com sucesso.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "pk05",
        }

    except Exception as e:
        log.error("Erro no upsert (pk05)", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro no upsert (pk05)", e)