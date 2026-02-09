from sqlalchemy import Column, String, Integer, DateTime, Float
from sqlalchemy.sql import func
from database.database import Base


class PK05(Base):
    id = Column(Integer, primary_key=True, index=True)
    supply_area = Column(String, nullable=False)
    deposit = Column(String, nullable=False)
    responsible = Column(String, nullable=False)
    discharge_point = Column(String, nullable=False)
    description = Column(String, nullable=False)
    takt = Column(String, nullable=False)
    
    created_at = Column(DateTime, server_default=func.now(), nullable=False)