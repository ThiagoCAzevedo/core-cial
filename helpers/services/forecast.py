from fastapi import Depends
from services.forecast.fx4pd import ReturnFX4PDValues
from services.forecast.buff_al import ReturnBuffAssemblyLineValues
from services.forecast.forecaster import DefineForecastValues
from database.queries import UpsertInfos
from database.database import get_db
from sqlalchemy.orm import Session
from helpers.log.logger import logger


class BuildPipeline:
    def __init__(self):
        self.log = logger("forecast")
        self.log.info("Initializing Forecast BuildPipeline")

    @staticmethod
    def build_forecast(svc: ReturnFX4PDValues):
        log = logger("forecast")
        log.info("Starting Forecast preparation pipeline")

        try:
            log.info("Creating FX4PD DataFrame")
            df = svc.create_fx4pd_df()
        except Exception:
            log.error("Error creating FX4PD DataFrame", exc_info=True)
            raise

        try:
            log.info("Renaming selected columns")
            df = svc.rename_select_columns(df)
        except Exception:
            log.error("Error renaming FX4PD columns", exc_info=True)
            raise

        try:
            log.info("Cleaning FX4PD DataFrame columns")
            df = svc.clean_column(df)
            log.info("Forecast pipeline completed successfully")
        except Exception:
            log.error("Error cleaning FX4PD DataFrame columns", exc_info=True)
            raise

        return df


class DependenciesInjection:
    log = logger("forecast")

    @staticmethod
    def get_fx4pd_service() -> ReturnFX4PDValues:
        DependenciesInjection.log.info("Creating service ReturnFX4PDValues")
        try:
            return ReturnFX4PDValues()
        except Exception:
            DependenciesInjection.log.error("Error creating ReturnFX4PDValues service", exc_info=True)
            raise

    @staticmethod
    def get_buff_al_service() -> ReturnBuffAssemblyLineValues:
        DependenciesInjection.log.info("Creating service ReturnBuffAssemblyLineValues")
        try:
            return ReturnBuffAssemblyLineValues()
        except Exception:
            DependenciesInjection.log.error("Error creating ReturnBuffAssemblyLineValues service", exc_info=True)
            raise

    @staticmethod
    def get_forecast_service() -> DefineForecastValues:
        DependenciesInjection.log.info("Creating service DefineForecastValues")
        try:
            return DefineForecastValues()
        except Exception:
            DependenciesInjection.log.error("Error creating DefineForecastValues service", exc_info=True)
            raise

    @staticmethod
    def get_upsert_service(db: Session = Depends(get_db)) -> UpsertInfos:
        DependenciesInjection.log.info("Creating UpsertInfos with active DB session")
        try:
            return UpsertInfos(db)
        except Exception:
            DependenciesInjection.log.error("Error creating UpsertInfos service", exc_info=True)
            raise