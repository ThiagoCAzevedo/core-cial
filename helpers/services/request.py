from fastapi import Depends
from services.request.requester import QuantityToRequest, LM01_Requester
from services.sap.session_manager import SAPSessionManager
from database.queries import UpsertInfos
from database.database import get_db
from sqlalchemy.orm import Session


class BuildPipeline:
    @staticmethod
    def build_to_request(svc: QuantityToRequest):
        return svc._define_diference_to_request()
    
    
class DependenciesInjection:
    @staticmethod
    def get_to_request() -> QuantityToRequest:
        return QuantityToRequest()
    
    @staticmethod
    def get_sap_session() -> SAPSessionManager:
        return SAPSessionManager()

    @staticmethod
    def get_upsert_service(db: Session = Depends(get_db)) -> UpsertInfos:
        return UpsertInfos(db)