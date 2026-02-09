from services.sap.session_manager import SAPSessionManager
from services.requests_checker.lt22 import LT22_Session, LT22_Selectors, LT22_Parameters, LT22_Submit

    
class BuildPipeline:
    @staticmethod
    def request(session):
        selectors = LT22_Selectors(session)
        selectors.expand()
        selectors.select()
        selectors.top()
        selectors.take()

        params = LT22_Parameters(session)
        params.set_deposit()
        params.set_b01()
        params.set_pending_only()
        params.set_dates_today()
        params.set_layout()

        submit = LT22_Submit(session)
        submit.submit()

        return True


class DependenciesInjection:
    @staticmethod
    def get_sap_session() -> SAPSessionManager:
        return SAPSessionManager()

    @staticmethod
    def get_lt22_session() -> LT22_Session:
        return LT22_Session()
