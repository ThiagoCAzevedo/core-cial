from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from modules.consumption.application.service import ConsumeValuesService
from database.session import get_db
from common.http_errors import http_500
from common.logger import logger


router = APIRouter()
log = logger("consumption")


def get_consume_service(db: Session = Depends(get_db)) -> ConsumeValuesService:
    return ConsumeValuesService(db)


@router.get("/response/to-consume", summary="Get values to consume")
def get_to_consume_response(svc: ConsumeValuesService = Depends(get_consume_service)):
    log.info("GET /consumption/response/to-consume — started getting values to consume")
    try:
        df = svc.values_to_consume()
        log.info(f"Successfully obtained values to consume — {df.height} rows")
        return df.to_dicts()
    except Exception as e:
        log.error("Error getting values to consume", exc_info=True)
        raise http_500("Error getting values to consume: ", e)


@router.put("/update/to-consume", summary="Update values consumed via external API")
def update_to_consume(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    svc: ConsumeValuesService = Depends(get_consume_service),
):
    log.info(f"PUT /consumption/update/to-consume — batch_size={batch_size}")

    try:
        df = svc.values_to_consume()
        log.info(f"Calculated consumption values — {df.height} rows to update")
        
        result = svc.update_infos(df=df, batch_size=batch_size)
        log.info(f"Successfully executed update via external PKMC API — {result['total_records']} records")

        return {
            "message": "Successfully updated consumption values via external PKMC API.",
            "data": result,
            "batch_size": batch_size,
        }

    except Exception as e:
        log.error("Error updating values to consume", exc_info=True)
        raise http_500("Error updating values to consume: ", e)
