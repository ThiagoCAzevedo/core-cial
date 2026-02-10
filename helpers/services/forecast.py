from fastapi import Depends
from services.forecast.fx4pd import ReturnFX4PDValues
from services.forecast.buff_al import ReturnBuffAssemblyLineValues
from services.forecast.forecaster import DefineForecastValues
from database.queries import UpsertInfos
from database.database import get_db
from sqlalchemy.orm import Session
from helpers.log.logger import logger


class BuildPipeline:
    def __init__(self):
        self.log = logger("forecast")
        self.log.info("Inicializando BuildPipeline de Forecast")

    @staticmethod
    def build_forecast(svc: ReturnFX4PDValues):
        log = logger("forecast")
        log.info("Iniciando pipeline de preparação do Forecast")

        try:
            log.info("Criando DataFrame FX4PD")
            df = svc.create_fx4pd_df()
        except Exception:
            log.error("Erro ao criar DataFrame FX4PD", exc_info=True)
            raise

        try:
            log.info("Renomeando colunas selecionadas")
            df = svc.rename_select_columns(df)
        except Exception:
            log.error("Erro ao renomear colunas", exc_info=True)
            raise

        try:
            log.info("Limpando colunas do DataFrame")
            df = svc.clean_column(df)
            log.info("Pipeline de Forecast concluído com sucesso")
        except Exception:
            log.error("Erro ao limpar colunas do DataFrame", exc_info=True)
            raise

        return df


class DependenciesInjection:
    log = logger("forecast")

    @staticmethod
    def get_fx4pd_service() -> ReturnFX4PDValues:
        DependenciesInjection.log.info("Criando serviço ReturnFX4PDValues")
        try:
            return ReturnFX4PDValues()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar ReturnFX4PDValues", exc_info=True)
            raise

    @staticmethod
    def get_buff_al_service() -> ReturnBuffAssemblyLineValues:
        DependenciesInjection.log.info("Criando serviço ReturnBuffAssemblyLineValues")
        try:
            return ReturnBuffAssemblyLineValues()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar ReturnBuffAssemblyLineValues", exc_info=True)
            raise

    @staticmethod
    def get_forecast_service() -> DefineForecastValues:
        DependenciesInjection.log.info("Criando serviço DefineForecastValues")
        try:
            return DefineForecastValues()
        except Exception:
            DependenciesInjection.log.error("Erro ao criar DefineForecastValues", exc_info=True)
            raise

    @staticmethod
    def get_upsert_service(db: Session = Depends(get_db)) -> UpsertInfos:
        DependenciesInjection.log.info("Criando UpsertInfos com sessão do banco")
        try:
            return UpsertInfos(db)
        except Exception:
            DependenciesInjection.log.error("Erro ao criar UpsertInfos", exc_info=True)
            raise