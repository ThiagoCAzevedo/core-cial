"""Domain entities for requests_closure module"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any


@dataclass
class ProcessedRequest:
    """Represents a processed request after closure"""
    partnumber: str
    supply_area: str
    num_reg_circ: str
    takt: str
    rack: str
    qty_requested: float
    qty_confirmed: Optional[float] = None
    status: str = "processed"  # processed, confirmed, rejected, pending
    notes: Optional[str] = None
    processed_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary representation"""
        return {
            "partnumber": self.partnumber,
            "supply_area": self.supply_area,
            "num_reg_circ": self.num_reg_circ,
            "takt": self.takt,
            "rack": self.rack,
            "qty_requested": self.qty_requested,
            "qty_confirmed": self.qty_confirmed,
            "status": self.status,
            "notes": self.notes,
            "processed_at": self.processed_at,
        }


@dataclass
class ClosureResult:
    """Represents the result of a closure operation"""
    closure_id: str
    total_requests: int
    successful_requests: int
    failed_requests: int
    status: str = "completed"  # completed, partial, failed
    requests: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary representation"""
        return {
            "closure_id": self.closure_id,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "status": self.status,
            "requests": self.requests,
            "errors": self.errors,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }

    def success_rate(self) -> float:
        """Calculate the success rate of the closure"""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100
