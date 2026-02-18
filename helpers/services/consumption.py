from fastapi import Depends
from services.consumption.consumer import ConsumeValues
from database.database import get_db
from helpers.log.logger import logger
from sqlalchemy.orm import Session


class DependeciesInjection:
    log = logger("consumption")

    @staticmethod
    def get_consume(db: Session = Depends(get_db)) -> ConsumeValues:
        DependeciesInjection.log.info("Creating instance of ConsumeValues")

        try:
            DependeciesInjection.log.info("ConsumeValues instance created successfully")
            return ConsumeValues(db)
        except Exception:
            DependeciesInjection.log.error("Error creating ConsumeValues instance", exc_info=True)
            raise