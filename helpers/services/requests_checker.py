from services.sap_manager.session_manager import SAPSessionManager
from services.requests_checker.sp02 import SP02_Session, SP02_Rows, SP02_Actions
from services.requests_checker.lt22 import LT22_Session, LT22_Selectors, LT22_Parameters, LT22_Submit
from helpers.log.logger import logger


# -- SP02 -- 
class SP02_BuildPipeline:
    def __init__(self):
        self.log = logger("requests_checker")
        self.log.info("Inicializando BuildPipeline SP02")

    @staticmethod
    def find_lt22(session):
        log = logger("requests_checker")
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


# -- LT22 --
class LT22_BuildPipeline:
    def __init__(self):
        self.log = logger("requests_checker")
        self.log.info("Inicializando BuildPipeline LT22")

    @staticmethod
    def request(session):
        log = logger("requests_checker")
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
    log = logger("requests_checker")

    @staticmethod
    def get_sap_session() -> SAPSessionManager:
        DependenciesInjection.log.info("Criando SAPSessionManager para SP02")
        try:
            return SAPSessionManager()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar SAPSessionManager", exc_info=True)
            raise
        
    # -- SP02 --
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

    # -- LT22 --
    @staticmethod
    def get_lt22_session() -> LT22_Session:
        DependenciesInjection.log.info("Criando LT22_Session")
        try:
            return LT22_Session()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar LT22_Session", exc_info=True)
            raise