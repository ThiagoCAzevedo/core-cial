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
        self.log.info("Inicializando BuildPipeline de Request")

    @staticmethod
    def build_to_request(svc: QuantityToRequest):
        log = logger("requests_builder")
        log.info("Iniciando pipeline de cálculo de quantidade a solicitar")

        try:
            result = svc._define_diference_to_request()
            log.info("Pipeline de cálculo finalizado com sucesso")
            return result

        except Exception:
            log.error("Erro ao definir diferença para request", exc_info=True)
            raise


class DependenciesInjection:
    log = logger("requests_builder")

    @staticmethod
    def get_to_request() -> QuantityToRequest:
        DependenciesInjection.log.info("Criando serviço QuantityToRequest")
        try:
            return QuantityToRequest()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar QuantityToRequest", exc_info=True)
            raise

    @staticmethod
    def get_lm01_requester():
        DependenciesInjection.log.info("Criando LM01_Requester")

        try:
            DependenciesInjection.log.info("Obtendo sessão SAP")
            sap = SAPSessionManager().get_session()

            DependenciesInjection.log.info("Calculando dataframe quantity_to_request")
            df = QuantityToRequest()._define_diference_to_request()

            requester = LM01_Requester(sap, df)
            DependenciesInjection.log.info("LM01_Requester criado com sucesso")

            return requester

        except Exception:
            DependenciesInjection.log.error("Erro ao criar LM01_Requester", exc_info=True)
            raise

    @staticmethod
    def get_sap_session() -> SAPSessionManager:
        DependenciesInjection.log.info("Criando SAPSessionManager para LM01")
        try:
            return SAPSessionManager()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar SAPSessionManager", exc_info=True)
            raise

    @staticmethod
    def get_upsert_service(db: Session = Depends(get_db)) -> UpsertInfos:
        DependenciesInjection.log.info("Criando serviço UpsertInfos para Requests")
        try:
            return UpsertInfos(db)
        except Exception:
            DependenciesInjection.log.error("Erro ao criar UpsertInfos", exc_info=True)
            raise