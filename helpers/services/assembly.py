from fastapi import Depends
from services.assembly.assembly_api import AccessAssemblyLineApi
from services.assembly.processor import DefineDataFrame, TransformDataFrame
from database.queries import UpsertInfos
from database.database import get_db
from sqlalchemy.orm import Session
from helpers.log.logger import logger


class BuildPipeline:
    def __init__(self):
        self.log = logger("assembly")
        self.log.info("Inicializando BuildPipeline")

    def build_assembly(self, api: AccessAssemblyLineApi):
        self.log.info("Iniciando pipeline de montagem de dataframe")

        try:
            self.log.info("Obtendo dados brutos da API de assembly line")
            raw = api.get_raw_response()
            self.log.info("Dados brutos obtidos com sucesso")

        except Exception:
            self.log.error("Erro ao obter dados brutos da API", exc_info=True)
            raise

        try:
            self.log.info("Extraindo registros de carro")
            df = DefineDataFrame(raw).extract_car_records()
            self.log.info("Extração concluída")

        except Exception:
            self.log.error("Erro ao extrair registros", exc_info=True)
            raise

        try:
            self.log.info("Transformando dataframe base")
            df = TransformDataFrame(df).transform()
            self.log.info("Transformação concluída")

        except Exception:
            self.log.error("Erro na transformação inicial do dataframe", exc_info=True)
            raise

        try:
            self.log.info("Atribuindo FX4PD ao dataframe")
            df = TransformDataFrame(df).attach_fx4pd()
            self.log.info("Atribuição FX4PD concluída")

        except Exception:
            self.log.error("Erro ao atribuir FX4PD", exc_info=True)
            raise

        try:
            self.log.info("Coletando dataframe final")
            final_df = df.collect()
            self.log.info("Pipeline assembly finalizado com sucesso")

            return final_df

        except Exception:
            self.log.error("Erro ao coletar dataframe final", exc_info=True)
            raise


class DependeciesInjection:
    log = logger("assembly")

    @staticmethod
    def get_api() -> AccessAssemblyLineApi:
        DependeciesInjection.log.info("Criando instância de AccessAssemblyLineApi")
        try:
            return AccessAssemblyLineApi()
        except Exception:
            DependeciesInjection.log.error("Erro ao criar AccessAssemblyLineApi", exc_info=True)
            raise

    @staticmethod
    def get_upsert(db: Session = Depends(get_db)) -> UpsertInfos:
        DependeciesInjection.log.info("Criando UpsertInfos com sessão de banco")
        try:
            return UpsertInfos(db)
        except Exception:
            DependeciesInjection.log.error("Erro ao criar UpsertInfos", exc_info=True)
            raise