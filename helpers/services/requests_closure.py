from fastapi import Depends
from services.requests_closure.processor import DefineDataFrame, CleanDataFrame
from services.requests_closure.update import UpdateDeleteValues
from database.database import get_db
from sqlalchemy.orm import Session
from helpers.log.logger import logger


class LT22_BuildPipeline:
    def __init__(self):
        self.log = logger("requests_closure")
        self.log.info("Inicializando BuildPipeline PK05")

    @staticmethod
    def build_lt22(raw_svc: DefineDataFrame, cleaner_svc: CleanDataFrame):
        log = logger("requests_closure")
        log.info("Iniciando pipeline PK05")

        try:
            log.info("Criando dataframe bruto PK05")
            df = raw_svc.create_lt22_df()
        except Exception:
            log.error("Erro ao criar dataframe bruto PK05", exc_info=True)
            raise

        try:
            log.info("Renomeando colunas do PK05")
            df = cleaner_svc.rename_columns(df)
        except Exception:
            log.error("Erro ao renomear colunas no PK05", exc_info=True)
            raise

        try:
            log.info("Criando colunas adicionais para PK05")
            df = cleaner_svc.change_columns_type(df)
        except Exception:
            log.error("Erro ao criar colunas adicionais no PK05", exc_info=True)
            raise

        try:
            log.info("Filtrando colunas do PK05")
            df = cleaner_svc.filter_values(df)
            log.info("Pipeline PK05 finalizado com sucesso")
        except Exception:
            log.error("Erro ao filtrar colunas do PK05", exc_info=True)
            raise

        return df


class DependenciesInjection:
    log = logger("requests_closue")
    
    @staticmethod
    def get_lt22() -> DefineDataFrame:
        DependenciesInjection.log.info("Criando serviço PK05_DefineDataframe")
        try:
            return DefineDataFrame()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar PK05_DefineDataframe", exc_info=True)
            raise

    @staticmethod
    def get_lt22_cleaner() -> CleanDataFrame:
        DependenciesInjection.log.info("Criando serviço PK05_Cleaner")
        try:
            return CleanDataFrame()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar PK05_Cleaner", exc_info=True)
            raise

    @staticmethod
    def get_upsert_service(db: Session = Depends(get_db)):
        return UpdateDeleteValues(db)

