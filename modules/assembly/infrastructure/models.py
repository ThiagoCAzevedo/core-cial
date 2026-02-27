from sqlalchemy import Column, String, Integer
from database.base import Base


class AssemblyModel(Base):
    __tablename__ = "assembly_line"

    id = Column(Integer, primary_key=True, autoincrement=True)
    knr = Column(String(20), index=True)
    model = Column(String(50))
    lfdnr_sequence = Column(String(50))
    werk = Column(String(10))
    spj = Column(String(10))
    lane = Column(String(20))
    takt = Column(String(10))
    knr_fx4pd = Column(String(50), index=True)