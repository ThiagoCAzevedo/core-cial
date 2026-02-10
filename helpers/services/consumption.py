from fastapi import Depends
from services.consumption.consumer import ConsumeValues
from database.database import get_db
from sqlalchemy.orm import Session
from helpers.log.logger import logger


class DependeciesInjection:
    log = logger("consumption")

    @staticmethod
    def get_consume(db: Session = Depends(get_db)) -> ConsumeValues:
        DependeciesInjection.log.info("Criando instância de ConsumeValues")

        try:
            service = ConsumeValues(db)
            DependeciesInjection.log.info("Instância de ConsumeValues criada com sucesso")
            return service

        except Exception:
            DependeciesInjection.log.error("Erro ao criar instância de ConsumeValues", exc_info=True)
            raise