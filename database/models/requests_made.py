from sqlalchemy import Column, String, DateTime, Float
from sqlalchemy.sql import func
from database.database import Base


class RequestsMade(Base):
    __tablename__ = "requests_made"
    
    partnumber = Column(String(255), nullable=False, primary_key=True)
    num_reg_circ = Column(String(255), nullable=False)
    supply_area = Column(String(255), nullable=False)
    qty_to_request = Column(Float, nullable=False)
    qty_boxes_to_request = Column(Float, nullable=False)
    takt = Column(String(255), nullable=False)
    rack = Column(String(255), nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)