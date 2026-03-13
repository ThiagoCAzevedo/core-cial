"""Domain entities for requests_checker module"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List


@dataclass
class LT22CheckResult:
    """Represents the result of checking a request in SAP LT22 transaction"""
    partnumber: str
    takt: str
    rack: str
    status: str  # success, failed, partial, pending
    message: str
    qty_confirmed: Optional[float] = None
    delivery_date: Optional[str] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary representation"""
        return {
            "partnumber": self.partnumber,
            "takt": self.takt,
            "rack": self.rack,
            "status": self.status,
            "message": self.message,
            "qty_confirmed": self.qty_confirmed,
            "delivery_date": self.delivery_date,
            "errors": self.errors,
            "warnings": self.warnings,
            "created_at": self.created_at,
        }

    def is_successful(self) -> bool:
        """Check if the result indicates a successful operation"""
        return self.status == "success"

    def add_error(self, error: str) -> None:
        """Add an error message"""
        self.errors.append(error)

    def add_warning(self, warning: str) -> None:
        """Add a warning message"""
        self.warnings.append(warning)


@dataclass
class CheckBatch:
    """Represents a batch of requests to check in LT22"""
    batch_id: str
    requests: List[dict]
    status: str = "pending"  # pending, checking, completed, failed
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary representation"""
        return {
            "batch_id": self.batch_id,
            "requests": self.requests,
            "status": self.status,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }
