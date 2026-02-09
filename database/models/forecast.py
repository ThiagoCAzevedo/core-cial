from sqlalchemy import Column, String, Integer, DateTime, Float
from sqlalchemy.sql import func
from database.database import Base


class Forecast(Base):
    __tablename__ = "forecast"

    partnumber = Column(String, nullable=False, primary_key=True)
    num_reg_circ = Column(String, nullable=False)
    takt = Column(String, nullable=False)
    rack = Column(String, nullable=False)
    lb_balance = Column(Float, nullable=False)
    total_theoretical_qty = Column(Float, nullable=False)
    qty_for_restock = Column(Float, nullable=False)
    qty_per_box = Column(Float, nullable=False)
    qty_max_box = Column(Float, nullable=False)
    knr_fx4pd = Column(String, nullable=False, primary_key=True)
    qty_usage = Column(Float, nullable=False)
    qty_unit = Column(Integer, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
