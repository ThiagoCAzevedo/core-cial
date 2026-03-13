"""Domain entities for consumption module"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class ConsumeValue:
    """Represents a consumption value for a product in a specific location"""
    partnumber: str
    takt: str
    rack: str
    knr_fx4pd: str
    qty_usage: float
    qty_unit: int
    lb_balance: float
    assembly_takt: Optional[str] = None
    pkmc_qty: Optional[float] = None
    created_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary representation"""
        return {
            "partnumber": self.partnumber,
            "takt": self.takt,
            "rack": self.rack,
            "knr_fx4pd": self.knr_fx4pd,
            "qty_usage": self.qty_usage,
            "qty_unit": self.qty_unit,
            "lb_balance": self.lb_balance,
            "assembly_takt": self.assembly_takt,
            "pkmc_qty": self.pkmc_qty,
            "created_at": self.created_at,
        }
