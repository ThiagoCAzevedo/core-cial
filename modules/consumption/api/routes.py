from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from modules.consumption.application.service import ConsumptionService
from database.session import get_db
from common.http_errors import http_500


router = APIRouter()


def get_service(db: Session = Depends(get_db)):
    return ConsumptionService(db)


@router.get("/response/to-consume", summary="Get values to consume")
def get_to_consume(
    service: ConsumptionService = Depends(get_service),
    limit: int = Query(100, ge=1),
):
    try:
        df = service.values_to_consume()
        return df.head(limit).to_dicts()
    except Exception as e:
        raise http_500("Error retrieving values to consume: ", e)


@router.put("/update/to-consume", summary="Update values consumed")
def update_to_consume(
    batch_size: int = Query(10_000, ge=1),
    service: ConsumptionService = Depends(get_service),
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
        raise http_500("Error updating consumption values: ", e)