from fastapi import Depends
from services.static.pk05 import PK05_Cleaner, PK05_DefineDataframe
from services.static.pkmc import PKMC_Cleaner, PKMC_DefineDataframe
from database.queries import UpsertInfos
from database.database import get_db
from sqlalchemy.orm import Session
from helpers.log.logger import logger


# -- PK05 --
class PK05_BuildPipeline:
    def __init__(self):
        self.log = logger("static")
        self.log.info("Initializing PK05 BuildPipeline")

    @staticmethod
    def build_pk05(raw_svc: PK05_DefineDataframe, cleaner_svc: PK05_Cleaner):
        log = logger("static")
        log.info("Starting PK05 pipeline")

        try:
            log.info("Creating raw PK05 DataFrame")
            df = raw_svc.create_df()
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
            df = cleaner_svc.create_columns(df)
        except Exception:
            log.error("Error creating additional PK05 columns", exc_info=True)
            raise

        try:
            log.info("Filtering PK05 rows")
            df = cleaner_svc.filter_columns(df)
            log.info("PK05 pipeline completed successfully")
        except Exception:
            log.error("Error filtering PK05 rows", exc_info=True)
            raise

        return df


# -- PKMC --
class PKMC_BuildPipeline:
    def __init__(self):
        self.log = logger("static")
        self.log.info("Initializing PKMC BuildPipeline")

    @staticmethod
    def build_pkmc(raw_svc: PKMC_DefineDataframe, cleaner_svc: PKMC_Cleaner):
        log = logger("static")
        log.info("Starting PKMC pipeline")

        try:
            log.info("Creating raw PKMC DataFrame")
            df = raw_svc.create_df()
        except Exception:
            log.error("Error creating raw PKMC DataFrame", exc_info=True)
            raise

        try:
            log.info("Renaming PKMC columns")
            df = cleaner_svc.rename_columns(df)
        except Exception:
            log.error("Error renaming PKMC columns", exc_info=True)
            raise

        try:
            log.info("Filtering PKMC rows")
            df = cleaner_svc.filter_columns(df)
        except Exception:
            log.error("Error filtering PKMC rows", exc_info=True)
            raise

        try:
            log.info("Cleaning PKMC columns")
            df = cleaner_svc.clean_columns(df)
        except Exception:
            log.error("Error cleaning PKMC columns", exc_info=True)
            raise

        try:
            log.info("Creating additional PKMC columns")
            df = cleaner_svc.create_columns(df)
            log.info("PKMC pipeline completed successfully")
        except Exception:
            log.error("Error creating additional PKMC columns", exc_info=True)
            raise

        return df


class DependenciesInjection:
    log = logger("static")

    # -- PK05 --
    @staticmethod
    def get_pk05() -> PK05_DefineDataframe:
        DependenciesInjection.log.info("Creating PK05_DefineDataframe service")
        try:
            return PK05_DefineDataframe()
        except Exception:
            DependenciesInjection.log.error("Error creating PK05_DefineDataframe service", exc_info=True)
            raise

    @staticmethod
    def get_pk05_cleaner() -> PK05_Cleaner:
        DependenciesInjection.log.info("Creating PK05_Cleaner service")
        try:
            return PK05_Cleaner()
        except Exception:
            DependenciesInjection.log.error("Error creating PK05_Cleaner service", exc_info=True)
            raise

    # -- PKMC --
    @staticmethod
    def get_pkmc() -> PKMC_DefineDataframe:
        DependenciesInjection.log.info("Creating PKMC_DefineDataframe service")
        try:
            return PKMC_DefineDataframe()
        except Exception:
            DependenciesInjection.log.error("Error creating PKMC_DefineDataframe service", exc_info=True)
            raise

    @staticmethod
    def get_pkmc_cleaner() -> PKMC_Cleaner:
        DependenciesInjection.log.info("Creating PKMC_Cleaner service")
        try:
            return PKMC_Cleaner()
        except Exception:
            DependenciesInjection.log.error("Error creating PKMC_Cleaner service", exc_info=True)
            raise

    @staticmethod
    def get_upsert_service(db: Session = Depends(get_db)) -> UpsertInfos:
        DependenciesInjection.log.info("Creating UpsertInfos service for static data")
        try:
            return UpsertInfos(db)
        except Exception:
            DependenciesInjection.log.error("Error creating UpsertInfos service", exc_info=True)
            raise