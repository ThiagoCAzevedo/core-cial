from sqlalchemy.orm import Session
from common.logger import logger
import polars as pl
from modules.forecast.infrastructure.pkmc_client import PKMC_Client
from modules.forecast.infrastructure.pk05_client import PK05_Client
from modules.forecast.infrastructure.repositories import (
    SQLAlchemyForecastRepository,
    ExternalClientsRepository,
)
from modules.forecast.application.use_cases import BuildForecastDataUseCase


class ForecastService:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("forecast")
        self._initialize_dependencies()
        self.log.info("Initializing ForecastService")

    def _initialize_dependencies(self) -> None:
        forecast_repo = SQLAlchemyForecastRepository(self.db)
        pkmc_client = PKMC_Client()
        pk05_client = PK05_Client()
        external_repo = ExternalClientsRepository(pkmc_client, pk05_client)
        
        self.forecast_usecase = BuildForecastDataUseCase(forecast_repo, external_repo)

    def join_fx4pd_pkmc_pk05(self) -> pl.LazyFrame:
        lf = self.forecast_usecase.execute()
        return lf
