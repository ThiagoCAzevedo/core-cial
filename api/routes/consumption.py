from fastapi import APIRouter, Query, Depends
from services.consumption.consumer import ConsumeValues
from helpers.services.consumption import DependeciesInjection
from helpers.services.http_exception import HTTP_Exceptions
from helpers.log.logger import logger


router = APIRouter()
log = logger("consumption")


@router.get("/response/to-consume", summary="Get values to consume")
def get_to_consume_response(svc: ConsumeValues = Depends(DependeciesInjection.get_consume)):
    log.info("GET /consumption/response/to-consume — started getting values to consume")
    try:
        log.info("Successfully obtained values to consume")
        return svc.values_to_consume().to_dicts()
    except Exception as e:
        log.error("Error getting values to consume", exc_info=True)
        raise HTTP_Exceptions().http_502("Error getting values to consume: ", e)


@router.put("/update/to-consume", summary="Update values consumed")
def update_to_consume(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    svc: ConsumeValues = Depends(DependeciesInjection.get_consume),
):
    log.info(f"PUT /consumption/update/to-consume — batch_size={batch_size}")

    try:
        df = svc.values_to_consume()
        print("df to consume:", df)
        updated_rows = svc._update_infos(df=df, batch_size=batch_size)
        log.info(f"Successfully executed update — amount of registers: {updated_rows}")

        return {
            "message": "Successfully executed update.",
            "rows_updated": updated_rows,
            "batch_size": batch_size,
        }

    except Exception as e:
        log.error("Error updating values to consume", exc_info=True)
        raise HTTP_Exceptions().http_502("Error updating values to consume: ", e)