from helpers.data.cleaner import CleanerBase
from helpers.log.logger import logger
import polars as pl


class PK05_DefineDataframe(CleanerBase):
    def __init__(self):
        self.log = logger("static")
        self.log.info("Inicializando PK05_DefineDataframe")

        CleanerBase.__init__(self)

    def create_df(self):
        self.log.info("Carregando arquivo do PK05_PATH como LazyFrame")

        try:
            df = self._load_file("PK05_PATH").lazy()
            self.log.info("LazyFrame criado com sucesso a partir do PK05_PATH")
            return df

        except Exception:
            self.log.error("Erro ao carregar arquivo PK05_PATH", exc_info=True)
            raise


class PK05_Cleaner(CleanerBase):
    def __init__(self):
        self.log = logger("static")
        self.log.info("Inicializando PK05_Cleaner")

        CleanerBase.__init__(self)

    def filter_columns(self, df):
        self.log.info("Aplicando filtro: deposit == 'LB01' AND takt startswith 'T'")

        try:
            df = df.filter(
                pl.col("deposit") == "LB01",
                (pl.col("takt").is_not_null() &
                 pl.col("takt").str.starts_with("T"))
            )
            self.log.info("Filtro aplicado com sucesso")
            return df

        except Exception:
            self.log.error("Erro ao aplicar filtro nas colunas do PK05", exc_info=True)
            raise
    
    def create_columns(self, df):
        self.log.info("Criando colunas derivadas: extraindo TAKT e adicionando row_index")

        try:
            df = df.with_columns(
                pl.col("description").str.extract(r"(T\d+)", 1).alias("takt")
            )

            df = df.with_row_index(name="id")

            self.log.info("Colunas criadas com sucesso")
            return df

        except Exception:
            self.log.error("Erro ao criar colunas para PK05", exc_info=True)
            raise

    def rename_columns(self, df):
        self.log.info("Renomeando colunas conforme dicionário padrão PK05")

        rename_map = {
            "Área abastec.prod.": "supply_area",
            "Depósito": "deposit",
            "Responsável": "responsible",
            "Ponto de descarga": "discharge_point",
            "Denominação SupM": "description",
        }

        try:
            df = self._rename(df, rename_map)
            self.log.info("Renomeação concluída com sucesso")
            return df

        except Exception:
            self.log.error("Erro ao renomear colunas no PK05", exc_info=True)
            raise