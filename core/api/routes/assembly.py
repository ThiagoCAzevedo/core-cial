from fastapi import APIRouter, Query, Depends
from services.assembly.assembly_api import AccessAssemblyLineApi
from helpers.services.assembly import BuildPipeline, DependeciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from helpers.log.logger import logger
from database.queries import UpsertInfos


router = APIRouter()
log = logger("assembly")


@router.get("/response/json", summary="Get response from Assembly Line API")
def get_json_response(
    api: AccessAssemblyLineApi = Depends(DependeciesInjection.get_api),
    limit: int = Query(1, ge=1, le=5),
):
    log.info("GET /assembly/response/json — started collecting JSON from Assembly Line API")

    try:
        log.info("Sucessfully returned JSON from Assembly Line API")
        return dict(list(api.get_json_response().items())[:limit])

    except Exception as e:
        log.error("Error trying to get JSON from Assembly Line API", exc_info=True)
        raise HTTP_Exceptions().http_502("Error trying to get JSON from Assembly Line API: ", e)


@router.get("/response/processed", summary="Get processed response from Assembly Line API JSON")
def get_processed_response(
    api: AccessAssemblyLineApi = Depends(DependeciesInjection.get_api),
    limit: int = Query(50, ge=1, le=100000)
):
    log.info(f"GET /assembly/response/processed — limit={limit}")

    try:
        df = BuildPipeline().build_assembly(api)
        log.info(f"Processing finished — amount of registers: {df.height}")
        return df.head(limit).to_dicts()

    except Exception as e:
        log.error("Error processing Assembly Line JSON", exc_info=True)
        raise HTTP_Exceptions().http_500("Error processing Assembly Line JSON: ", e)


@router.post("/upsert", summary="Upsert JSON values from Assembly Line in database")
def upsert_assembly(
    api: AccessAssemblyLineApi = Depends(DependeciesInjection.get_api),
    upsert: UpsertInfos = Depends(DependeciesInjection.get_upsert),
    batch_size: int = Query(10000, ge=1, le=100000)
):
    log.info(f"POST /assembly/upsert — batch_size={batch_size}")

    try:
        df = BuildPipeline().build_assembly(api)
        log.info(f"Assembly pipeline successfully executed — amount of registers: {df.height}")

        rows = upsert.upsert_df("assembly_line", df, batch_size)
        log.info(f"Successfully upserted values — amount of registers: {rows}")

        return {
            "message": "Successfully upserted values",
            "rows": rows,
            "batch_size": batch_size,
            "table": "assembly_line",
        }

    except Exception as e:
        log.error("Error during upsert operation", exc_info=True)
        raise HTTP_Exceptions().http_500("Error during upsert operation: ", e)