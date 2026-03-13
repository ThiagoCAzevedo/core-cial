from common.utils.cleaner import CleanerBase
from common.logger import logger
import polars as pl


class CleanDataFrame(CleanerBase):
    def __init__(self):
        self.log = logger("requests_closure")
        self.log.info("Initializing CleanDataFrame")
        super().__init__()

    def clean_columns(self, df):
        self.log.info("Cleaning and sanitizing columns: partnumber, num_ot")
        try:
            
            df = df.with_columns(
                pl.col("partnumber")
                    .cast(pl.Utf8)
                    .str.strip()
                    .str.replace_all(r"\s+", "")
                    .str.replace_all(r"\.", "")
                    .str.replace_all(r"[^\w-]", "")
                    .str.to_uppercase(),

                pl.col("num_ot")
                    .cast(pl.Utf8)
                    .str.strip()
                    .str.replace_all(r"\s+", "")
                    .str.replace_all(r"\.", "")
                    .str.replace_all(r"[^\w-]", "")
                    .str.to_uppercase()
            )

            df = df.filter(
                (pl.col("partnumber").is_not_null() & (pl.col("partnumber") != "")) |
                (pl.col("num_ot").is_not_null() & (pl.col("num_ot") != ""))
            )

            self.log.info("Columns cleaned successfully (empty rows removed)")
            return df

        except Exception:
            self.log.error("Error cleaning columns", exc_info=True)
            raise

    def change_columns_type(self, df):
        self.log.info("Casting request_date and request_hour columns")
        try:
            
            df = df.with_columns(
                pl.col("request_date").str.strptime(pl.Date, format="%d.%m.%Y", strict=False),
                pl.col("request_hour").str.strptime(pl.Time, format="%H:%M:%S", strict=False)
            )
            self.log.info("Column type conversion completed")
            return df
        except Exception:
            self.log.error("Error converting column types", exc_info=True)
            raise

    def rename_columns(self, df):
        self.log.info("Renaming columns according to map")
        try:
            rename_map = {
                "Nº OT": "num_ot",
                "Material": "partnumber",
                "Hora": "request_hour",
                "Dt.criação": "request_date",
            }
            df_collected = df.collect()
            df = self._rename(df_collected, rename_map)
            self.log.info("Columns renamed successfully")
            return df.lazy()
        except Exception:
            self.log.error("Error renaming columns", exc_info=True)
            raise
