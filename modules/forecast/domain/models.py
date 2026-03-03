from sqlalchemy import Column, String, Integer, Float, DateTime, Index
from sqlalchemy.sql import func
from database.base import Base


class FX4PD(Base):
    __tablename__ = "fx4pd"

    knr_fx4pd = Column(String(255), nullable=False, primary_key=True)
    partnumber = Column(String(255), nullable=False, primary_key=True)
    qty_usage = Column(Float, nullable=False)
    qty_unit = Column(Integer, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class Forecast(Base):
    __tablename__ = "forecast"

    partnumber = Column(String(50), nullable=False, index=True, primary_key=True)
    num_reg_circ = Column(String(20), nullable=False)
    takt = Column(String(10), nullable=False, index=True, primary_key=True)
    rack = Column(String(10), nullable=False, index=True, primary_key=True)
    lb_balance = Column(Float, nullable=False)
    total_theoretical_qty = Column(Float, nullable=False)
    qty_for_restock = Column(Float, nullable=False)
    qty_per_box = Column(Float, nullable=False)
    qty_max_box = Column(Float, nullable=False)
    knr_fx4pd = Column(String(50), nullable=False, index=True, primary_key=True)
    qty_usage = Column(Float, nullable=False)
    qty_unit = Column(Integer, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
