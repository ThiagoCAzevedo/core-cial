from fastapi import Depends
from services.static.pk05 import PK05_Cleaner, PK05_DefineDataframe
from services.static.pkmc import PKMC_Cleaner, PKMC_DefineDataframe
from database.queries import UpsertInfos
from database.database import get_db
from sqlalchemy.orm import Session
from helpers.log.logger import logger


# -- PK05 --
class PK05_BuildPipeline:
    def __init__(self):
        self.log = logger("static")
        self.log.info("Inicializando BuildPipeline PK05")

    @staticmethod
    def build_pk05(raw_svc: PK05_DefineDataframe, cleaner_svc: PK05_Cleaner):
        log = logger("static")
        log.info("Iniciando pipeline PK05")

        try:
            log.info("Criando dataframe bruto PK05")
            df = raw_svc.create_df()
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
            df = cleaner_svc.create_columns(df)
        except Exception:
            log.error("Erro ao criar colunas adicionais no PK05", exc_info=True)
            raise

        try:
            log.info("Filtrando colunas do PK05")
            df = cleaner_svc.filter_columns(df)
            log.info("Pipeline PK05 finalizado com sucesso")
        except Exception:
            log.error("Erro ao filtrar colunas do PK05", exc_info=True)
            raise

        return df


# -- PKMC --
class PKMC_BuildPipeline:
    def __init__(self):
        self.log = logger("static")
        self.log.info("Inicializando BuildPipeline PKMC")

    @staticmethod
    def build_pkmc(raw_svc: PKMC_DefineDataframe, cleaner_svc: PKMC_Cleaner):
        log = logger("static")
        log.info("Iniciando pipeline PKMC")

        try:
            log.info("Criando dataframe bruto PKMC")
            df = raw_svc.create_df()
        except Exception:
            log.error("Erro ao criar dataframe bruto PKMC", exc_info=True)
            raise

        try:
            log.info("Renomeando colunas PKMC")
            df = cleaner_svc.rename_columns(df)
        except Exception:
            log.error("Erro ao renomear colunas PKMC", exc_info=True)
            raise

        try:
            log.info("Filtrando colunas PKMC")
            df = cleaner_svc.filter_columns(df)
        except Exception:
            log.error("Erro ao filtrar colunas PKMC", exc_info=True)
            raise

        try:
            log.info("Limpando colunas PKMC")
            df = cleaner_svc.clean_columns(df)
        except Exception:
            log.error("Erro ao limpar colunas PKMC", exc_info=True)
            raise

        try:
            log.info("Criando colunas adicionais PKMC")
            df = cleaner_svc.create_columns(df)
            log.info("Pipeline PKMC finalizado com sucesso")
        except Exception:
            log.error("Erro ao criar colunas adicionais PKMC", exc_info=True)
            raise

        return df


class DependenciesInjection:
    log = logger("static")
    
    # -- PK05 --
    @staticmethod
    def get_pk05() -> PK05_DefineDataframe:
        DependenciesInjection.log.info("Criando serviço PK05_DefineDataframe")
        try:
            return PK05_DefineDataframe()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar PK05_DefineDataframe", exc_info=True)
            raise

    @staticmethod
    def get_pk05_cleaner() -> PK05_Cleaner:
        DependenciesInjection.log.info("Criando serviço PK05_Cleaner")
        try:
            return PK05_Cleaner()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar PK05_Cleaner", exc_info=True)
            raise

    # -- PKMC --
    @staticmethod
    def get_pkmc() -> PKMC_DefineDataframe:
        DependenciesInjection.log.info("Criando serviço PKMC_DefineDataframe")
        try:
            return PKMC_DefineDataframe()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar PKMC_DefineDataframe", exc_info=True)
            raise

    @staticmethod
    def get_pkmc_cleaner() -> PKMC_Cleaner:
        DependenciesInjection.log.info("Criando serviço PKMC_Cleaner")
        try:
            return PKMC_Cleaner()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar PKMC_Cleaner", exc_info=True)
            raise


    @staticmethod
    def get_upsert_service(db: Session = Depends(get_db)) -> UpsertInfos:
        DependenciesInjection.log.info("Criando serviço UpsertInfos para PK05")
        try:
            return UpsertInfos(db)
        except Exception:
            DependenciesInjection.log.error("Erro ao criar UpsertInfos", exc_info=True)
            raise