from sqlalchemy import Column, String, DateTime, Float, PrimaryKeyConstraint
from sqlalchemy.sql import func
from database.base import Base


class RequestsMade(Base):
    __tablename__ = "requests_made"
    
    partnumber = Column(String(255), nullable=False, primary_key=True)
    supply_area = Column(String(255), nullable=False, primary_key=True)
    num_reg_circ = Column(String(255), nullable=False)
    qty_to_request = Column(Float, nullable=False)
    qty_boxes_to_request = Column(Float, nullable=False)
    takt = Column(String(255), nullable=False)
    rack = Column(String(255), nullable=False)
    num_shipment = Column(String(255), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
