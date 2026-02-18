from fastapi import Depends
from sqlalchemy.orm import Session
from services.assembly.assembly_api import AccessAssemblyLineApi
from services.assembly.processor import DefineLazyFrame, TransformLazyFrame
from helpers.log.logger import logger
from database.queries import UpsertInfos
from database.database import get_db


class BuildPipeline:
    def __init__(self):
        self.log = logger("assembly")
        self.log.info("Initializing BuildPipeline")

    def build_assembly(self, api: AccessAssemblyLineApi):
        self.log.info("Starting LazyFrame assembly pipeline")

        try:
            self.log.info("Retrieving raw assembly line API data")
            raw = api.get_json_response()
            self.log.info("Raw data retrieved successfully")
        except Exception:
            self.log.error("Error retrieving raw API data", exc_info=True)
            raise

        try:
            self.log.info("Extracting car records")
            lf = DefineLazyFrame(raw).extract_car_records()
            self.log.info("Record extraction completed")
        except Exception:
            self.log.error("Error extracting car records", exc_info=True)
            raise

        try:
            self.log.info("Transforming base LazyFrame")
            lf = TransformLazyFrame(lf).transform()
            self.log.info("Base transformation completed")
        except Exception:
            self.log.error("Error during initial LazyFrame transformation", exc_info=True)
            raise

        try:
            self.log.info("Attaching FX4PD values to LazyFrame")
            lf = TransformLazyFrame(lf).attach_fx4pd()
            self.log.info("FX4PD attachment completed")
        except Exception:
            self.log.error("Error attaching FX4PD", exc_info=True)
            raise

        try:
            self.log.info("Collecting final DataFrame")
            return lf.collect()
        except Exception:
            self.log.error("Error collecting final DataFrame", exc_info=True)
            raise


class DependeciesInjection:
    log = logger("assembly")

    @staticmethod
    def get_api() -> AccessAssemblyLineApi:
        DependeciesInjection.log.info("Creating instance of AccessAssemblyLineApi")
        try:
            return AccessAssemblyLineApi()
        except Exception:
            DependeciesInjection.log.error("Error creating AccessAssemblyLineApi instance", exc_info=True)
            raise

    @staticmethod
    def get_upsert(db: Session = Depends(get_db)) -> UpsertInfos:
        DependeciesInjection.log.info("Creating UpsertInfos with DB session")
        try:
            return UpsertInfos(db)
        except Exception:
            DependeciesInjection.log.error("Error creating UpsertInfos instance", exc_info=True)
            raise