"""Domain entities for requests_builder module"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class OrderRequest:
    """Represents an order request to be submitted to SAP"""
    partnumber: str
    supply_area: str
    num_reg_circ: str
    takt: str
    rack: str
    qty_to_request: float
    qty_boxes_to_request: float
    lb_balance: Optional[float] = None
    total_theoretical_qty: Optional[float] = None
    qty_for_restock: Optional[float] = None
    qty_per_box: Optional[float] = None
    qty_max_box: Optional[float] = None
    num_shipment: Optional[str] = None
    created_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary representation"""
        return {
            "partnumber": self.partnumber,
            "supply_area": self.supply_area,
            "num_reg_circ": self.num_reg_circ,
            "takt": self.takt,
            "rack": self.rack,
            "qty_to_request": self.qty_to_request,
            "qty_boxes_to_request": self.qty_boxes_to_request,
            "lb_balance": self.lb_balance,
            "total_theoretical_qty": self.total_theoretical_qty,
            "qty_for_restock": self.qty_for_restock,
            "qty_per_box": self.qty_per_box,
            "qty_max_box": self.qty_max_box,
            "num_shipment": self.num_shipment,
            "created_at": self.created_at,
        }


@dataclass
class LM01Request:
    """Represents an LM01 transaction to be submitted to SAP"""
    partnumber: str
    num_reg_circ: str
    qty_to_request: float
    qty_boxes_to_request: float
    takt: str
    rack: str
    num_shipment: Optional[str] = None
    status: str = "pending"
    created_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary representation"""
        return {
            "partnumber": self.partnumber,
            "num_reg_circ": self.num_reg_circ,
            "qty_to_request": self.qty_to_request,
            "qty_boxes_to_request": self.qty_boxes_to_request,
            "takt": self.takt,
            "rack": self.rack,
            "num_shipment": self.num_shipment,
            "status": self.status,
            "created_at": self.created_at,
        }
