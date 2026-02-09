from fastapi import Depends
from services.consumption.consumer import ConsumeValues
from database.database import get_db
from sqlalchemy.orm import Session
from services.consumption.consumer import ConsumeValues


class DependeciesInjection:
    @staticmethod
    def get_consume(db: Session = Depends(get_db)) -> ConsumeValues:
        return ConsumeValues(db)
