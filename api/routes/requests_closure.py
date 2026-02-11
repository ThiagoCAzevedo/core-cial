from fastapi import APIRouter, Query, Depends
from services.requests_closure.processor import DefineDataFrame, CleanDataFrame
from services.requests_closure.update import UpdateDeleteValues
from helpers.services.requests_closure import DependenciesInjection, LT22_BuildPipeline
from helpers.services.http_exception import HTTP_Exceptions
from helpers.log.logger import logger


router = APIRouter()
log = logger("requests_closure")


@router.get("/response/processed/lt22", summary="Get Cleaned LT22 Values")
def get_processed_lt22(
    raw_svc: DefineDataFrame = Depends(DependenciesInjection.get_lt22),
    cleaner_svc: CleanDataFrame = Depends(DependenciesInjection.get_lt22_cleaner),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"Rota /response/processed chamada — limit={limit}")

    try:
        df = LT22_BuildPipeline.build_lt22(raw_svc, cleaner_svc).head(limit).collect()
        log.info(f"PK05 processado com sucesso — total de registros exibidos: {df.height}")
        return df.to_dicts()

    except Exception as e:
        log.error("Erro ao processar PK05 (clean)", exc_info=True)
        raise HTTP_Exceptions().http_502("Erro ao processar PK05 (clean): ", e)


@router.post("/update-delete", summary="Atualiza lb_balance e remove RequestsMade")
def update_and_delete_lt22(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    raw_svc: DefineDataFrame = Depends(DependenciesInjection.get_lt22),
    cleaner_svc: CleanDataFrame = Depends(DependenciesInjection.get_lt22_cleaner),
    update_delete_svc: UpdateDeleteValues = Depends(DependenciesInjection.get_upsert_service),
):
    log.info(f"Rota POST /lt22/update-delete chamada — batch_size={batch_size}")

    try:
        df = LT22_BuildPipeline.build_lt22(raw_svc, cleaner_svc)
        log.info(f"LT22 processado — total de registros: {df.select(pl.len()).collect().item()}")

        update_delete_svc.update_lb_balance(df)
        log.info("Update de lb_balance realizado com sucesso")

        update_delete_svc.delete_requests_made(df)
        log.info("Delete em RequestsMade realizado com sucesso")

        return {
            "message": "Update e Delete realizados com sucesso.",
            "table_update": "pkmc",
            "table_delete": "requests_made",
            "batch_size": batch_size,
        }

    except Exception as e:
        log.error("Erro na rota update-delete (LT22)", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro ao executar update/delete LT22", e)
