from sqlalchemy.orm import Session
from common.logger import logger
import polars as pl
from modules.consumption.infrastructure.pkmc_adapter import PKMC_Client
from modules.consumption.infrastructure.repositories import (
    SQLAlchemyConsumptionRepository,
    PKMCExternalRepository,
)
from modules.consumption.application.use_cases import (
    CalculateConsumptionUseCase,
    UpdateConsumptionUseCase,
)


class ConsumeValuesService:
    """Service to calculate and apply consumption values from Forecast and Assembly data
    
    This service delegates business logic to use cases and accesses data via repositories,
    reducing direct coupling to infrastructure.
    """

    def __init__(self, db: Session):
        self.db = db
        self.log = logger("consumption")
        self._initialize_dependencies()
        self.log.info("Initializing ConsumeValuesService")

    def _initialize_dependencies(self) -> None:
        """Initialize repositories and use cases"""
        # Create repositories
        consumption_repo = SQLAlchemyConsumptionRepository(self.db)
        pkmc_client = PKMC_Client()
        external_repo = PKMCExternalRepository(pkmc_client)
        
        # Create use cases with injected dependencies
        self.calc_usecase = CalculateConsumptionUseCase(
            consumption_repo, external_repo
        )
        self.update_usecase = UpdateConsumptionUseCase(external_repo)

    def values_to_consume(self) -> pl.DataFrame:
        """Calculate consumption values via use case"""
        return self.calc_usecase.execute()

    def update_infos(self, df: pl.DataFrame, batch_size: int = 10000) -> dict:
        """Update PKMC via use case
        
        Args:
            df: DataFrame with partnumber and updated lb_balance
            batch_size: Number of records to send per API request
            
        Returns:
            Result from external API
        """
        return self.update_usecase.execute(df, batch_size)

