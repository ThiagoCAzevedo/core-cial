from fastapi import Depends
from services.requests_builder.requester import LM01_Requester
from services.requests_builder.to_request import QuantityToRequest
from services.sap_manager.session_manager import SAPSessionManager
from database.queries import UpsertInfos
from database.database import get_db
from sqlalchemy.orm import Session
from helpers.log.logger import logger


class BuildPipeline:
    def __init__(self):
        self.log = logger("requests_builder")
        self.log.info("Initializing Request BuildPipeline")

    @staticmethod
    def build_to_request(svc: QuantityToRequest):
        log = logger("requests_builder")
        log.info("Starting pipeline to calculate quantities to request")

        try:
            result = svc._define_diference_to_request()
            log.info("Quantity-to-request pipeline completed successfully")
            return result

        except Exception:
            log.error("Error defining request difference", exc_info=True)
            raise


class DependenciesInjection:
    log = logger("requests_builder")

    @staticmethod
    def get_to_request() -> QuantityToRequest:
        DependenciesInjection.log.info("Creating QuantityToRequest service")
        try:
            return QuantityToRequest()
        except Exception:
            DependenciesInjection.log.error("Error creating QuantityToRequest", exc_info=True)
            raise

    @staticmethod
    def get_lm01_requester():
        DependenciesInjection.log.info("Creating LM01_Requester")

        try:
            DependenciesInjection.log.info("Retrieving SAP session")
            sap = SAPSessionManager().get_session()

            DependenciesInjection.log.info("Calculating quantity_to_request DataFrame")
            df = QuantityToRequest()._define_diference_to_request()

            requester = LM01_Requester(sap, df)
            DependenciesInjection.log.info("LM01_Requester created successfully")

            return requester

        except Exception:
            DependenciesInjection.log.error("Error creating LM01_Requester", exc_info=True)
            raise

    @staticmethod
    def get_sap_session() -> SAPSessionManager:
        DependenciesInjection.log.info("Creating SAPSessionManager for LM01")
        try:
            return SAPSessionManager()
        except Exception:
            DependenciesInjection.log.error("Error creating SAPSessionManager", exc_info=True)
            raise

    @staticmethod
    def get_upsert_service(db: Session = Depends(get_db)) -> UpsertInfos:
        DependenciesInjection.log.info("Creating UpsertInfos service for Requests")
        try:
            return UpsertInfos(db)
        except Exception:
            DependenciesInjection.log.error("Error creating UpsertInfos service", exc_info=True)
            raise