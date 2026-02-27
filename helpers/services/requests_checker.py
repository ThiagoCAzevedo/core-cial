from fastapi import Depends
from services.sap_manager.session_manager import SAPSessionManager
from services.requests_checker.lt22 import LT22_Session, LT22_Parameters, LT22_Submit, ReturnValuesRequestsMade
from helpers.log.logger import logger
from sqlalchemy.orm import Session
from services.requests_checker.verifier_extractor import lt22_has_data


# -- LT22 --
class LT22_BuildPipeline():
    def __init__(self, db):
        self.log = logger("requests_checker")
        self.log.info("Initializing LT22 BuildPipeline")
        self.db = db

    def request(self, session):
        log = logger("requests_checker")
        log.info("Starting LT22 pipeline")

        try:
            num_shipments = DependenciesInjection.get_num_shipment(self.db)
            log.info(f"Found {len(num_shipments)} OT(s) to process.")
        except Exception:
            log.error("Error fetching num_shipment values", exc_info=True)
            return False
        
        try:
            num_partnumbers = DependenciesInjection.get_partnumber(self.db)
            log.info(f"Found {len(num_partnumbers)} OT(s) to process.")
        except Exception:
            log.error("Error fetching num_partnumber values", exc_info=True)
            return False


        for i in range(len(num_shipments)):
            try:
                log.info("Configuring LT22 parameters")
                params = LT22_Parameters(session)
                params.set_deposit()
                params.set_shipment(num_shipments[i])
                params.set_partnumber(num_partnumbers[i])
                params.set_b01()
                params.set_confirmed_only()

            except Exception:
                continue 

            try:
                submit = LT22_Submit(session)
                submit.submit()

            except Exception:
                continue

        lt22_has_data(session)

        return True



class DependenciesInjection:
    log = logger("requests_checker")

    @staticmethod
    def get_num_shipment(db: Session):
        return ReturnValuesRequestsMade(db).get_all_num_shipment()

    @staticmethod
    def get_partnumber(db: Session):
        return ReturnValuesRequestsMade(db).get_all_partnumber()

    @staticmethod
    def get_sap_session() -> SAPSessionManager:
        DependenciesInjection.log.info("Creating SAPSessionManager for SP02")
        try:
            return SAPSessionManager()
        except Exception:
            DependenciesInjection.log.error("Error creating SAPSessionManager", exc_info=True)
            raise
        
        
    # -- LT22 --
    @staticmethod
    def get_lt22_session(sap_mgr: SAPSessionManager = Depends(get_sap_session)) -> LT22_Session:
        DependenciesInjection.log.info("Creating LT22_Session")
        try:
            sap = sap_mgr.get_session()
            return LT22_Session(sap)
        except Exception:
            DependenciesInjection.log.error("Error creating LT22_Session", exc_info=True)
            raise
