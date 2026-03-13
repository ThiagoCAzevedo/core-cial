"""Repository pattern for forecast module"""
from abc import ABC, abstractmethod
import polars as pl
from modules.forecast.domain.models import FX4PD


class ForecastRepository(ABC):
    """Abstract repository for forecast data access"""
    
    @abstractmethod
    def get_fx4pd_data(self) -> pl.LazyFrame:
        """Get FX4PD data from database"""
        pass


class SQLAlchemyForecastRepository(ForecastRepository):
    """SQLAlchemy implementation of forecast repository"""
    
    def __init__(self, db):
        self.db = db
    
    def get_fx4pd_data(self) -> pl.LazyFrame:
        """Retrieve FX4PD data from database"""
        from sqlalchemy import select
        
        stmt = select(
            FX4PD.knr_fx4pd,
            FX4PD.partnumber,
            FX4PD.qty_usage,
            FX4PD.qty_unit,
        )
        rows = list(map(dict, self.db.execute(stmt).mappings()))
        return pl.from_dicts(rows).lazy()


class ExternalDataRepository(ABC):
    """Abstract repository for external data"""
    
    @abstractmethod
    def get_pkmc_data(self) -> pl.LazyFrame:
        """Get PKMC data from external API"""
        pass
    
    @abstractmethod
    def get_pk05_data(self) -> pl.LazyFrame:
        """Get PK05 data from external API"""
        pass


class ExternalClientsRepository(ExternalDataRepository):
    """External clients implementation"""
    
    def __init__(self, pkmc_client, pk05_client):
        self.pkmc_client = pkmc_client
        self.pk05_client = pk05_client
    
    def get_pkmc_data(self) -> pl.LazyFrame:
        """Fetch PKMC data"""
        return self.pkmc_client.get_all()
    
    def get_pk05_data(self) -> pl.LazyFrame:
        """Fetch PK05 data"""
        return self.pk05_client.get_all()
