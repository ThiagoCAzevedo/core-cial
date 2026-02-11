from services.sap_manager.session_manager import SAPSessionManager
from services.requests_checker.sp02 import SP02_Session, SP02_Rows, SP02_Actions
from services.requests_checker.lt22 import LT22_Session, LT22_Selectors, LT22_Parameters, LT22_Submit
from helpers.log.logger import logger


# -- SP02 -- 
class SP02_BuildPipeline:
    def __init__(self):
        self.log = logger("requests_checker")
        self.log.info("Initializing SP02 BuildPipeline")

    @staticmethod
    def find_lt22(session):
        log = logger("requests_checker")
        log.info("Starting LT22 search in SP02")

        try:
            rows = SP02_Rows(session)
            log.info("SP02_Rows instance created")

            result = rows.find_lt22_job()
            log.info("LT22 search completed successfully")

            return result

        except Exception:
            log.error("Error searching for LT22 job in SP02", exc_info=True)
            raise


# -- LT22 --
class LT22_BuildPipeline:
    def __init__(self):
        self.log = logger("requests_checker")
        self.log.info("Initializing LT22 BuildPipeline")

    @staticmethod
    def request(session):
        log = logger("requests_checker")
        log.info("Starting LT22 pipeline")

        try:
            log.info("Executing LT22 selectors")
            selectors = LT22_Selectors(session)
            selectors.expand()
            selectors.select()
            selectors.top()
            selectors.take()
            log.info("LT22 selectors completed")

        except Exception:
            log.error("Error executing LT22 selectors", exc_info=True)
            raise

        try:
            log.info("Configuring LT22 parameters")
            params = LT22_Parameters(session)
            params.set_deposit()
            params.set_b01()
            params.set_pending_only()
            params.set_dates_today()
            params.set_layout()
            log.info("LT22 parameters configured")

        except Exception:
            log.error("Error configuring LT22 parameters", exc_info=True)
            raise

        try:
            log.info("Submitting LT22 request")
            submit = LT22_Submit(session)
            submit.submit()
            log.info("LT22 request submitted successfully")

        except Exception:
            log.error("Error submitting LT22 request", exc_info=True)
            raise

        return True


class DependenciesInjection:
    log = logger("requests_checker")

    @staticmethod
    def get_sap_session() -> SAPSessionManager:
        DependenciesInjection.log.info("Creating SAPSessionManager for SP02")
        try:
            return SAPSessionManager()
        except Exception:
            DependenciesInjection.log.error("Error creating SAPSessionManager", exc_info=True)
            raise
        
    # -- SP02 --
    @staticmethod
    def get_sp02_session() -> SP02_Session:
        DependenciesInjection.log.info("Creating SP02_Session")
        try:
            return SP02_Session()
        except Exception:
            DependenciesInjection.log.error("Error creating SP02_Session", exc_info=True)
            raise

    @staticmethod
    def get_sp02_actions() -> SP02_Actions:
        DependenciesInjection.log.info("Creating SP02_Actions")
        try:
            return SP02_Actions()
        except Exception:
            DependenciesInjection.log.error("Error creating SP02_Actions", exc_info=True)
            raise

    # -- LT22 --
    @staticmethod
    def get_lt22_session() -> LT22_Session:
        DependenciesInjection.log.info("Creating LT22_Session")
        try:
            return LT22_Session()
        except Exception:
            DependenciesInjection.log.error("Error creating LT22_Session", exc_info=True)
            raise