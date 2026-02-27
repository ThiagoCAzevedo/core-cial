from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from modules.assembly.infrastructure.http_client import AssemblyApiClient
from modules.assembly.application.pipeline import AssemblyPipeline
from modules.assembly.infrastructure.repository import AssemblyRepository
from database.session import get_db
from common.http_errors import http_500


router = APIRouter()


def get_client():
    return AssemblyApiClient()


@router.get("/json")
def get_json(
    client: AssemblyApiClient = Depends(get_client),
    limit: int = Query(1, ge=1, le=5)
):
    try:
        return dict(list(client.get_json().items())[:limit])
    except Exception as e:
        raise http_500("Error getting JSON: ", e)
    

@router.get("/processed")
def get_processed(
    limit: int = Query(50, ge=1),
    client: AssemblyApiClient = Depends(get_client)
):
    try:
        df = AssemblyPipeline().run(client)
        return df.head(limit).to_dicts()
    except Exception as e:
        raise http_500("Error processing data: ", e)


@router.post("/upsert")
def upsert(
    batch_size: int = Query(5000, ge=100),
    client: AssemblyApiClient = Depends(get_client),
    db: Session = Depends(get_db)
):
    try:
        df = AssemblyPipeline().run(client)
        repo = AssemblyRepository(db)

        records = df.to_dicts()
        total = repo.bulk_upsert(records, batch_size)

        return {"upserted_rows": total}

    except Exception as e:
        raise http_500("Upsert error: ", e)