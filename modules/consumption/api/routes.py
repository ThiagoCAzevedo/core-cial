from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from modules.consumption.application.service import ConsumptionService
from database.session import get_db
from common.http_errors import http_500


router = APIRouter()


def get_service(db: Session = Depends(get_db)):
    return ConsumptionService(db)


@router.get("/response/to-consume")
def get_to_consume_response(
    service: ConsumptionService = Depends(get_service)
):
    try:
        df = service.values_to_consume()
        return df.to_dicts()
    except Exception as e:
        raise http_500("Error getting values to consume: ", e)


@router.put("/update/to-consume")
def update_to_consume(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    service: ConsumptionService = Depends(get_service)
):
    try:
        df = service.values_to_consume()
        updated_rows = service.update_infos(df=df, batch_size=batch_size)

        return {
            "message": "Successfully executed update.",
            "rows_updated": updated_rows,
            "batch_size": batch_size,
        }

    except Exception as e:
        raise http_500("Error updating values to consume: ", e)