from helpers.data.cleaner import CleanerBase
from helpers.log.logger import logger
import polars as pl
import re


class DefineDataFrame(CleanerBase):
    def __init__(self):
        self.log = logger("lt22")
        self.log.info("Initializing DefineDataFrame")
        CleanerBase.__init__(self)

    def create_lt22_df(self):
        self.log.info("Loading LT22 file and searching for header row")

        try:
            rows_to_skip = 0
            while True:
                df = self._load_file("LT22_PATH", rows_to_skip=rows_to_skip, separator="\t")
                
                if any("OT" in col for col in df.columns):
                    self.log.info(f"Header found after skipping {rows_to_skip} rows")
                    break
                rows_to_skip += 1

            self.log.info("LT22 DataFrame loaded successfully as LazyFrame")
            return df.lazy()

        except Exception:
            self.log.error("Error loading LT22 file in create_lt22_df()", exc_info=True)
            raise


class CleanDataFrame(CleanerBase):
    def __init__(self):
        self.log = logger("lt22")
        self.log.info("Initializing CleanDataFrame")
        CleanerBase.__init__(self)

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

            self.log.info("Columns cleaned successfully")
            return df

        except Exception:
            self.log.error("Error cleaning columns in CleanDataFrame.clean_columns()", exc_info=True)
            raise

    def filter_values(self, df):
        self.log.info("Filtering rows with deposit_type == 'B01'")

        try:
            df = df.filter(pl.col("deposit_type") == "B01")
            self.log.info("Row filtering completed")
            return df

        except Exception:
            self.log.error("Error filtering values in filter_values()", exc_info=True)
            raise

    def change_columns_type(self, df):
        self.log.info("Casting request_date and request_hour columns to proper types")

        try:
            df = df.with_columns(
                pl.col("request_date").str.strptime(pl.Date, format="%d.%m.%Y", strict=False),
                pl.col("request_hour").str.strptime(pl.Time, format="%H:%M:%S", strict=False)
            )

            self.log.info("Column type conversion completed successfully")
            return df

        except Exception:
            self.log.error("Error converting column types in change_columns_type()", exc_info=True)
            raise

    def rename_columns(self, df):
        self.log.info("Renaming FX22 columns according to rename map")

        try:
            rename_map = {
                "N� OT": "num_ot",
                "Material": "partnumber",
                "Hora": "request_hour",
                "Dt.cria��o": "request_date",
                "Tp._duplicated_0": "deposit_type",
                "Posi�Dest": "supply_area"
            }

            df = self._rename(df, rename_map)
            self.log.info("Columns renamed successfully")
            return df

        except Exception:
            self.log.error("Error renaming columns in rename_columns()", exc_info=True)
            raise