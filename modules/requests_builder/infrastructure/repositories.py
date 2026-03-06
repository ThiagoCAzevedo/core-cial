from sqlalchemy import text
from abc import ABC, abstractmethod
from typing import List, Optional
import polars as pl
from modules.requests_builder.domain.models import RequestsMade


class RequestsRepository(ABC):
    """Abstract repository for requests data access"""
    
    @abstractmethod
    def upsert_requests(self, records: list[dict]) -> int:
        """Insert or update request records"""
        pass
    
    @abstractmethod
    def get_all_requests(self) -> List[dict]:
        """Get all request records"""
        pass


class SQLAlchemyRequestsRepository(RequestsRepository):
    """SQLAlchemy implementation of requests repository"""
    
    def __init__(self, db):
        self.db = db
    
    def upsert_requests(self, records: list[dict]) -> int:
        """Upsert request records into database"""
        from sqlalchemy.dialects.mysql import insert
        
        stmt = insert(RequestsMade).values(records)
        on_duplicate = {
            col.name: stmt.inserted[col.name]
            for col in RequestsMade.__table__.columns
            if col.name not in ["id", "created_at", "updated_at", "num_shipment"]
        }
        stmt = stmt.on_duplicate_key_update(on_duplicate)
        
        self.db.execute(stmt)
        self.db.commit()
        return len(records)
    
    def get_all_requests(self) -> List[dict]:
        query_str = f"SELECT * FROM requests_made"
        
        result = self.db.execute(text(query_str))
        records = [dict(row) for row in result.mappings().all()]
        return records


class ExternalDataRepository(ABC):
    """Abstract repository for external data sources"""
    
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
