from fastapi import Depends
from services.pkmc.pkmc import PKMC_Cleaner, PKMC_DefineDataframe
from database.queries import UpsertInfos
from database.database import get_db
from sqlalchemy.orm import Session
from helpers.log.logger import logger


class BuildPipeline:
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
        DependenciesInjection.log.info("Criando serviço UpsertInfos para PKMC")
        try:
            return UpsertInfos(db)
        except Exception:
            DependenciesInjection.log.error("Erro ao criar UpsertInfos", exc_info=True)
            raise