from sqlalchemy import Column, String, DateTime, Index
from sqlalchemy.sql import func
from database.base import Base


class Assembly(Base):
    __tablename__ = "assembly_line"

    knr = Column(String(20), nullable=False, primary_key=True)
    takt = Column(String(10), nullable=False, index=True)
    model = Column(String(10), nullable=False)
    lfdnr_sequence = Column(String(50), nullable=False)
    lane = Column(String(20), nullable=False, index=True)
    spj = Column(String(10), nullable=False)
    werk = Column(String(10), nullable=False)
    knr_fx4pd = Column(String(50), nullable=False, index=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)