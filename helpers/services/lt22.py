from services.sap.session_manager import SAPSessionManager
from services.requests_checker.lt22 import (
    LT22_Session,
    LT22_Selectors,
    LT22_Parameters,
    LT22_Submit,
)
from helpers.log.logger import logger


class BuildPipeline:
    def __init__(self):
        self.log = logger("sap")
        self.log.info("Inicializando BuildPipeline LT22")

    @staticmethod
    def request(session):
        log = logger("sap")
        log.info("Iniciando pipeline LT22")

        try:
            log.info("Executando seletores LT22")
            selectors = LT22_Selectors(session)
            selectors.expand()
            selectors.select()
            selectors.top()
            selectors.take()
            log.info("Seletores concluídos")

        except Exception:
            log.error("Erro ao executar seletores LT22", exc_info=True)
            raise

        try:
            log.info("Configurando parâmetros LT22")
            params = LT22_Parameters(session)
            params.set_deposit()
            params.set_b01()
            params.set_pending_only()
            params.set_dates_today()
            params.set_layout()
            log.info("Parâmetros LT22 configurados")

        except Exception:
            log.error("Erro ao configurar parâmetros LT22", exc_info=True)
            raise

        try:
            log.info("Submetendo requisição LT22")
            submit = LT22_Submit(session)
            submit.submit()
            log.info("Requisição LT22 submetida com sucesso")

        except Exception:
            log.error("Erro ao submeter requisição LT22", exc_info=True)
            raise

        return True


class DependenciesInjection:
    log = logger("sap")

    @staticmethod
    def get_sap_session() -> SAPSessionManager:
        DependenciesInjection.log.info("Criando SAPSessionManager")
        try:
            return SAPSessionManager()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar SAPSessionManager", exc_info=True)
            raise

    @staticmethod
    def get_lt22_session() -> LT22_Session:
        DependenciesInjection.log.info("Criando LT22_Session")
        try:
            return LT22_Session()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar LT22_Session", exc_info=True)
            raise
