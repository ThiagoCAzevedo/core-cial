from common.utils.cleaner import CleanerBase
from common.logger import logger
import polars as pl


class FX4PDService(CleanerBase):
    """Service to load and process FX4PD data"""

    def __init__(self):
        self.log = logger("forecast")
        self.log.info("Initializing FX4PDService")
        super().__init__()

    def pipeline(self) -> pl.DataFrame:
        """Execute full FX4PD pipeline: load -> rename -> clean"""
        self.log.info("Starting FX4PDService pipeline")

        try:
            lf = self.create_fx4pd_df()
        except Exception:
            self.log.error("Error creating FX4PD DataFrame", exc_info=True)
            raise

        try:
            lf = self.rename_select_columns(lf)
        except Exception:
            self.log.error("Error renaming FX4PD columns", exc_info=True)
            raise

        try:
            df = self.clean_column(lf)
            self.log.info("FX4PDService pipeline completed successfully")
        except Exception:
            self.log.error("Error cleaning FX4PD DataFrame columns", exc_info=True)
            raise

        return df

    def create_fx4pd_df(self) -> pl.LazyFrame:
        """Load FX4PD file as LazyFrame"""
        self.log.info("Loading FX4PD_PATH file as LazyFrame")
        try:
            lf = self._load_file("FX4PD_PATH").lazy()
            self.log.info("LazyFrame successfully created from FX4PD_PATH")
            return lf
        except Exception:
            self.log.error("Error loading FX4PD_PATH file", exc_info=True)
            raise

    def rename_select_columns(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Rename FX4PD DataFrame columns"""
        self.log.info("Renaming FX4PD DataFrame columns")
        try:
            df_collected = df.collect()
            rename_map = {
                df_collected.columns[0]: "knr_fx4pd",
                df_collected.columns[1]: "partnumber",
                df_collected.columns[5]: "qty_usage",
                df_collected.columns[6]: "qty_unit",
            }
            df = self._rename(df_collected, rename_map).lazy()
            self.log.info("Columns renamed successfully")
            return df
        except Exception:
            self.log.error("Error renaming columns in FX4PD", exc_info=True)
            raise

    def clean_column(self, df: pl.LazyFrame) -> pl.DataFrame:
        """Clean and cast FX4PD columns"""
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
                qty_usage=pl.col("qty_usage").cast(pl.Float64, strict=False).fill_null(0.0),
                qty_unit=pl.col("qty_unit").cast(pl.Int32, strict=False).fill_null(0),
            )

            self.log.info("Columns cleaned and converted successfully")
            return df.collect()
        except Exception:
            self.log.error("Error cleaning FX4PD columns", exc_info=True)
            raise