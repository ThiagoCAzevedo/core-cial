from helpers.data.cleaner import CleanerBase
from helpers.log.logger import logger
import polars as pl


class ReturnFX4PDValues(CleanerBase):
    def __init__(self):
        self.log = logger("forecast")
        self.log.info("Initializing ReturnFX4PDValues")

        CleanerBase.__init__(self)

    def create_fx4pd_df(self):
        self.log.info("Loading FX4PD_PATH file as LazyFrame")

        try:
            df = self._load_file("FX4PD_PATH").lazy()
            self.log.info("LazyFrame successfully created from FX4PD_PATH")
            return df

        except Exception:
            self.log.error("Error loading FX4PD_PATH file", exc_info=True)
            raise
    
    def rename_select_columns(self, df):
        self.log.info("Renaming FX4PD DataFrame columns")

        try:
            rename_map = {
                df.columns[0]: "knr_fx4pd",
                df.columns[1]: "partnumber",
                df.columns[5]: "qty_usage",
                df.columns[6]: "qty_unit",
            }

            df = self._rename(df, rename_map)
            self.log.info("Columns renamed successfully")
            return df

        except Exception:
            self.log.error("Error renaming columns in FX4PD", exc_info=True)
            raise
    
    def clean_column(self, df: pl.LazyFrame | pl.DataFrame):
        self.log.info("Starting cleaning and transformation of FX4PD columns")

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

            self.log.info("Columns cleaned and converted successfully")
            return df

        except Exception:
            self.log.error("Error cleaning FX4PD columns", exc_info=True)
            raise