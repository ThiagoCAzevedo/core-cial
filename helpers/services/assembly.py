from fastapi import Depends
from services.assembly.assembly_api import AccessAssemblyLineApi
from services.assembly.processor import DefineDataFrame, TransformDataFrame
from database.queries import UpsertInfos
from database.database import get_db
from sqlalchemy.orm import Session
from helpers.log.logger import logger


class BuildPipeline:
    def __init__(self):
        self.log = logger("assembly")
        self.log.info("Initializing BuildPipeline")

    def build_assembly(self, api: AccessAssemblyLineApi):
        self.log.info("Starting DataFrame assembly pipeline")

        try:
            self.log.info("Retrieving raw assembly line API data")
            raw = api.get_raw_response()
            self.log.info("Raw data retrieved successfully")

        except Exception:
            self.log.error("Error retrieving raw API data", exc_info=True)
            raise

        try:
            self.log.info("Extracting car records")
            df = DefineDataFrame(raw).extract_car_records()
            self.log.info("Record extraction completed")

        except Exception:
            self.log.error("Error extracting car records", exc_info=True)
            raise

        try:
            self.log.info("Transforming base DataFrame")
            df = TransformDataFrame(df).transform()
            self.log.info("Base transformation completed")

        except Exception:
            self.log.error("Error during initial DataFrame transformation", exc_info=True)
            raise

        try:
            self.log.info("Attaching FX4PD values to DataFrame")
            df = TransformDataFrame(df).attach_fx4pd()
            self.log.info("FX4PD attachment completed")

        except Exception:
            self.log.error("Error attaching FX4PD", exc_info=True)
            raise

        try:
            self.log.info("Collecting final DataFrame")
            return df

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