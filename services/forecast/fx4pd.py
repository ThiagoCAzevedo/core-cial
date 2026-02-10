from helpers.data.cleaner import CleanerBase
from helpers.log.logger import logger
import polars as pl


class ReturnFX4PDValues(CleanerBase):
    def __init__(self):
        self.log = logger("forecast")
        self.log.info("Inicializando ReturnFX4PDValues")

        CleanerBase.__init__(self)

    def create_fx4pd_df(self):
        self.log.info("Carregando arquivo FX4PD_PATH como LazyFrame")

        try:
            df = self._load_file("FX4PD_PATH").lazy()
            self.log.info("LazyFrame criado com sucesso a partir do FX4PD_PATH")
            return df

        except Exception:
            self.log.error("Erro ao carregar arquivo FX4PD_PATH", exc_info=True)
            raise
    
    def rename_select_columns(self, df):
        self.log.info("Renomeando colunas do DataFrame FX4PD")

        try:
            rename_map = {
                df.columns[0]: "knr_fx4pd",
                df.columns[1]: "partnumber",
                df.columns[5]: "qty_usage",
                df.columns[6]: "qty_unit",
            }

            df = self._rename(df, rename_map)
            self.log.info("Colunas renomeadas com sucesso")
            return df

        except Exception:
            self.log.error("Erro ao renomear colunas no FX4PD", exc_info=True)
            raise
    
    def clean_column(self, df: pl.LazyFrame | pl.DataFrame):
        self.log.info("Iniciando limpeza e transformação de colunas do FX4PD")

        try:
            df = df.with_columns(
                pl.col(pl.Utf8).str.replace_all(" ", "")
            )

            df = df.filter(
                pl.col("qty_usage")
                .cast(pl.Float64, strict=False)
                .is_not_null()
            )

            df = df.with_columns(
                qty_usage = pl.col("qty_usage").cast(pl.Float64, strict=False).fill_null(0.0),
                qty_unit  = pl.col("qty_unit").cast(pl.Int32,  strict=False).fill_null(0),
            )

            self.log.info("Colunas limpas e convertidas com sucesso")
            return df

        except Exception:
            self.log.error("Erro ao limpar colunas do FX4PD", exc_info=True)
            raise