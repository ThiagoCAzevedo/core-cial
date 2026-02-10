from services.sap.session_manager import SAPSessionManager
from services.requests_checker.sp02 import SP02_Session, SP02_Rows, SP02_Actions
from helpers.log.logger import logger


class BuildPipeline:
    def __init__(self):
        self.log = logger("sap")
        self.log.info("Inicializando BuildPipeline SP02")

    @staticmethod
    def find_lt22(session):
        log = logger("sap")
        log.info("Iniciando busca por LT22 no SP02")

        try:
            rows = SP02_Rows(session)
            log.info("Instância SP02_Rows criada")

            result = rows.find_lt22_job()
            log.info("Busca por LT22 concluída com sucesso")

            return result

        except Exception:
            log.error("Erro ao buscar job LT22 no SP02", exc_info=True)
            raise


class DependenciesInjection:
    log = logger("sap")

    @staticmethod
    def get_sap_session() -> SAPSessionManager:
        DependenciesInjection.log.info("Criando SAPSessionManager para SP02")
        try:
            return SAPSessionManager()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar SAPSessionManager", exc_info=True)
            raise

    @staticmethod
    def get_sp02_session() -> SP02_Session:
        DependenciesInjection.log.info("Criando SP02_Session")
        try:
            return SP02_Session()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar SP02_Session", exc_info=True)
            raise

    @staticmethod
    def get_sp02_actions() -> SP02_Actions:
        DependenciesInjection.log.info("Criando SP02_Actions")
        try:
            return SP02_Actions()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar SP02_Actions", exc_info=True)
            raise