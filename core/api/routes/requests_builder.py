from fastapi import APIRouter, Query, Depends
from services.requests_builder.requester import LM01_Requester
from services.requests_builder.to_request import QuantityToRequest
from helpers.services.requests_builder import DependenciesInjection, BuildPipeline
from helpers.services.http_exception import HTTP_Exceptions
from database.queries import UpsertInfos
from helpers.log.logger import logger


router = APIRouter()
log = logger("requests_builder")


@router.get("/response/to-request", summary="Get values to request")
def get_to_request(
    svc: QuantityToRequest = Depends(DependenciesInjection.get_to_request),
    limit: int = Query(50, ge=1, le=1000),
):
    log.info(f"GET /requests-builder/response/to-request — limit={limit}")

    try:
        df = svc._define_diference_to_request().collect()
        log.info(f"Values to request successfully loaded — total rows: {df.height}")
        return df.head(limit).to_dicts()

    except Exception as e:
        log.error("Error fetching values to request", exc_info=True)
        raise HTTP_Exceptions().http_502("Error fetching values to request: ", e)


@router.post("/upsert/to-request", summary="Upsert 'to request' values into the database")
def upsert_to_request(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    svc: QuantityToRequest = Depends(DependenciesInjection.get_to_request),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    log.info(f"POST /requests-builder/upsert/to-request — batch_size={batch_size}")

    try:
        df = BuildPipeline.build_to_request(svc)
        log.info(f"Values processed — total rows: {len(df)}")

        rows = upsert_svc.upsert_df("requests_made", df, batch_size)
        log.info(f"Upsert completed — rows inserted: {rows}")

        return {
            "message": "Upsert 'to request' completed successfully.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "requests_made",
        }

    except Exception as e:
        log.error("Error during upsert (to request)", exc_info=True)
        raise HTTP_Exceptions().http_500("Error during upsert (to request)", e)


@router.post("/requester", summary="Send 'to request' values to SAP LM01")
def requester(
    svc: LM01_Requester = Depends(DependenciesInjection.get_lm01_requester),
    sap=Depends(DependenciesInjection.get_sap_session)
):
    log.info("POST /requests-builder/requester — starting SAP requester execution")

    try:
        session = sap.get_session()
        log.info("SAP session retrieved from SessionManager")

        if not session:
            log.error("No active SAP session found")
            raise HTTP_Exceptions().http_400(
                "No active SAP session.",
                "Before using this endpoint, execute /sap/session to open a SAP session."
            )

        rows = svc._request_lm01()
        log.info(f"SAP requester executed — total rows processed: {rows}")

        return {
            "message": "Requester completed successfully.",
            "rows": rows,
        }

    except Exception as e:
        log.error("Error in requester (to request)", exc_info=True)
        raise HTTP_Exceptions().http_500("Error in requester (to request)", e)