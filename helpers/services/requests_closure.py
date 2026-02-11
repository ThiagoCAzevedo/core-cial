from fastapi import Depends
from services.requests_closure.processor import DefineDataFrame, CleanDataFrame
from services.requests_closure.update import UpdateDeleteValues
from database.database import get_db
from sqlalchemy.orm import Session
from helpers.log.logger import logger


class LT22_BuildPipeline:
    def __init__(self):
        self.log = logger("requests_closure")
        self.log.info("Initializing PK05 BuildPipeline")

    @staticmethod
    def build_lt22(raw_svc: DefineDataFrame, cleaner_svc: CleanDataFrame):
        log = logger("requests_closure")
        log.info("Starting PK05 pipeline")

        try:
            log.info("Creating raw PK05 DataFrame")
            df = raw_svc.create_lt22_df()
        except Exception:
            log.error("Error creating raw PK05 DataFrame", exc_info=True)
            raise

        try:
            log.info("Renaming PK05 columns")
            df = cleaner_svc.rename_columns(df)
        except Exception:
            log.error("Error renaming PK05 columns", exc_info=True)
            raise

        try:
            log.info("Creating additional PK05 columns")
            df = cleaner_svc.change_columns_type(df)
        except Exception:
            log.error("Error creating additional PK05 columns", exc_info=True)
            raise

        try:
            log.info("Filtering PK05 values")
            df = cleaner_svc.filter_values(df)
            log.info("PK05 pipeline completed successfully")
        except Exception:
            log.error("Error filtering PK05 DataFrame", exc_info=True)
            raise

        return df


class DependenciesInjection:
    log = logger("requests_closure")
    
    @staticmethod
    def get_lt22() -> DefineDataFrame:
        DependenciesInjection.log.info("Creating PK05 DefineDataFrame service")
        try:
            return DefineDataFrame()
        except Exception:
            DependenciesInjection.log.error("Error creating PK05 DefineDataFrame service", exc_info=True)
            raise

    @staticmethod
    def get_lt22_cleaner() -> CleanDataFrame:
        DependenciesInjection.log.info("Creating PK05 CleanDataFrame service")
        try:
            return CleanDataFrame()
        except Exception:
            DependenciesInjection.log.error("Error creating PK05 CleanDataFrame service", exc_info=True)
            raise

    @staticmethod
    def get_upsert_service(db: Session = Depends(get_db)):
        return UpdateDeleteValues(db)