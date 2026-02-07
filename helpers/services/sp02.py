from services.sap.session_manager import SAPSessionManager
from services.requests_checker.sp02  import SP02_Session, SP02_Rows, SP02_Actions


class BuildPipeline:
    @staticmethod
    def find_lt22(session):
        rows = SP02_Rows(session)
        return rows.find_lt22_job()
    

class DependenciesInjection:
    @staticmethod
    def get_sap_session() -> SAPSessionManager:
        return SAPSessionManager()

    @staticmethod
    def get_sp02_session() -> SP02_Session:
        return SP02_Session()

    @staticmethod
    def get_sp02_actions() -> SP02_Actions:
        return SP02_Actions()