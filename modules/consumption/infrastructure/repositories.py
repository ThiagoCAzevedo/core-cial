"""Repository pattern for consumption module - abstracts data access"""
from abc import ABC, abstractmethod
from typing import List, Optional
import polars as pl
from modules.consumption.domain.entities import ConsumeValue
from modules.forecast.domain.models import Forecast
from modules.assembly.domain.models import Assembly


class ConsumptionRepository(ABC):
    """Abstract repository for consumption data access"""
    
    @abstractmethod
    def get_forecast_data(self) -> pl.LazyFrame:
        """Get forecast data from database"""
        pass
    
    @abstractmethod
    def get_assembly_data(self) -> pl.LazyFrame:
        """Get assembly data from database"""
        pass


class SQLAlchemyConsumptionRepository(ConsumptionRepository):
    """SQLAlchemy implementation of consumption repository"""
    
    def __init__(self, db):
        self.db = db
    
    def get_forecast_data(self) -> pl.LazyFrame:
        """Retrieve forecast data from database"""
        from sqlalchemy import select
        
        stmt = select(
            Forecast.partnumber,
            Forecast.takt,
            Forecast.rack,
            Forecast.knr_fx4pd,
            Forecast.qty_usage,
        )
        rows = self.db.execute(stmt).mappings().all()

        columns = {key: [row[key] for row in rows] for key in rows[0].keys()}

        df = pl.DataFrame(columns)
        return df.lazy()
    
    def get_assembly_data(self) -> pl.LazyFrame:
        from sqlalchemy import select

        stmt = select(
            Assembly.knr_fx4pd,
            Assembly.takt.label("assembly_takt"),
        )

        rows = self.db.execute(stmt).mappings().all()

        columns = {key: [row[key] for row in rows] for key in rows[0].keys()}

        df = pl.DataFrame(columns)

        return df.lazy()

class ExternalDataRepository(ABC):
    """Abstract repository for external API data"""
    
    @abstractmethod
    def get_pkmc_data(self) -> pl.LazyFrame:
        """Get PKMC data from external API"""
        pass
    
    @abstractmethod
    def update_pkmc_data(self, records: list[dict]) -> dict:
        """Update PKMC data via external API"""
        pass


class PKMCExternalRepository(ExternalDataRepository):
    """PKMC client implementation of external data repository"""
    
    def __init__(self, pkmc_client):
        self.pkmc_client = pkmc_client
    
    def get_pkmc_data(self) -> pl.LazyFrame:
        """Fetch PKMC data from external API"""
        return self.pkmc_client.get_all()
    
    def update_pkmc_data(self, records: list[dict]) -> dict:
        """Update PKMC records via external API"""
        return self.pkmc_client.update(records)
