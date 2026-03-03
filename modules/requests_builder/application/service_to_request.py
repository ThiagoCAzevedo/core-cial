from sqlalchemy.orm import Session
from common.logger import logger
import polars as pl
from modules.requests_builder.infrastructure.pkmc_adapter import PKMC_Client
from modules.requests_builder.infrastructure.pk05_adapter import PK05_Client
from modules.requests_builder.infrastructure.repositories import (
    SQLAlchemyRequestsRepository,
    ExternalClientsRepository,
)
from modules.requests_builder.application.use_cases import (
    CalculateOrderQuantitiesUseCase,
    SaveOrderRequestsUseCase,
)


class QuantityToRequestService:
    """Service to calculate and manage quantities to request
    
    This service delegates business logic to use cases and accesses data via repositories,
    reducing direct coupling to infrastructure.
    """

    def __init__(self, db: Session):
        self.db = db
        self.log = logger("requests_builder")
        self._initialize_dependencies()
        self.log.info("Initializing QuantityToRequestService")

    def _initialize_dependencies(self) -> None:
        """Initialize repositories and use cases"""
        # Create repositories
        requests_repo = SQLAlchemyRequestsRepository(self.db)
        pkmc_client = PKMC_Client()
        pk05_client = PK05_Client()
        external_repo = ExternalClientsRepository(pkmc_client, pk05_client)
        
        # Create use cases with injected dependencies
        self.calc_usecase = CalculateOrderQuantitiesUseCase(external_repo)
        self.save_usecase = SaveOrderRequestsUseCase(requests_repo)

    def define_diference_to_request(self) -> pl.LazyFrame:
        """Calculate quantities to request via use case"""
        return self.calc_usecase.execute()

    def upsert_to_request(self, batch_size: int = 10000) -> int:
        """Upsert quantity-to-request values into database via use case"""
        lf = self.define_diference_to_request()
        return self.save_usecase.execute(lf, batch_size)
