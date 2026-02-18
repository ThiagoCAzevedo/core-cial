from sqlalchemy import Column, String, Integer, DateTime, Float
from sqlalchemy.sql import func
from database.database import Base


class Assembly(Base):
    __tablename__ = "assembly_line"

    knr = Column(String(255), nullable=False, primary_key=True)
    takt = Column(String(255), nullable=False)
    model = Column(String(255), nullable=False)
    lfdnr_sequence = Column(String(255), nullable=False)
    lane = Column(String(255), nullable=False)
    spj = Column(Integer, nullable=False)
    werk = Column(Integer, nullable=False)
    knr_fx4pd = Column(String(255), nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)