from sqlalchemy import Column, String, Integer, DateTime, Float
from sqlalchemy.sql import func
from database.database import Base


class FX4PD(Base):
    __tablename__ = "fx4pd"

    knr_fx4pd = Column(String(255), nullable=False, primary_key=True)
    partnumber = Column(String(255), nullable=False, primary_key=True)
    qty_usage = Column(Float, nullable=False, primary_key=True)
    qty_unit = Column(Integer, nullable=False, primary_key=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)