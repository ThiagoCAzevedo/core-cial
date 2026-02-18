from sqlalchemy import Column, String, Integer, DateTime, Float
from sqlalchemy.sql import func
from database.database import Base


class PKMC(Base):
    __tablename__ = "pkmc"

    id = Column(Integer, primary_key=True, index=True)
    partnumber = Column(String(255), nullable=False)
    supply_area = Column(String(255), nullable=False)
    num_reg_circ = Column(String(255), nullable=False)
    deposit_type = Column(String(255), nullable=False)
    deposit_position = Column(String(255), nullable=False)
    container = Column(String(255))
    description = Column(String(255), nullable=False)
    pack_standard = Column(String(255))
    qty_per_box = Column(Float, nullable=False)
    qty_max_box = Column(Float, nullable=False)
    total_theoretical_qty = Column(Float, nullable=False)
    qty_for_restock = Column(Float, nullable=False)
    rack = Column(String(255), nullable=False)
    lb_balance = Column(Float, nullable=False)
    lb_balance_box = Column(Float, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    