from common.utils.cleaner import CleanerBase
from common.logger import logger
from sqlalchemy import select, delete as sql_delete
from sqlalchemy.orm import Session
from database.models.requests_made import RequestsMade
from modules.external_clients.pkmc_client import PKMC_Client
import polars as pl
import re


class DefineDataFrame(CleanerBase):
    """Load and parse LT22 exported data"""

    def __init__(self):
        self.log = logger("requests_closure")
        self.log.info("Initializing DefineDataFrame")
        super().__init__()

    def create_lt22_df(self) -> pl.LazyFrame:
        """Load LT22 file as LazyFrame"""
        self.log.info("Loading LT22 file and searching for header row")
        try:
            rows_to_skip = 0
            while True:
                df = self._load_file("LT22_PATH", rows_to_skip=rows_to_skip, separator="\t")
                if any("OT" in col for col in df.columns):
                    self.log.info(f"Header found after skipping {rows_to_skip} rows")
                    break
                rows_to_skip += 1

            self.log.info("LT22 DataFrame loaded successfully")
            return df.lazy()
        except Exception:
            self.log.error("Error loading LT22 file", exc_info=True)
            raise


class CleanDataFrame(CleanerBase):
    """Clean and validate LT22 data"""

    def __init__(self):
        self.log = logger("requests_closure")
        self.log.info("Initializing CleanDataFrame")
        super().__init__()

    def clean_columns(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Clean partnumber and num_ot columns"""
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
            self.log.error("Error cleaning columns", exc_info=True)
            raise

    def filter_values(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Filter rows with deposit_type == 'B01'"""
        self.log.info("Filtering rows with deposit_type == 'B01'")
        try:
            df = df.filter(pl.col("deposit_type") == "B01")
            self.log.info("Row filtering completed")
            return df
        except Exception:
            self.log.error("Error filtering values", exc_info=True)
            raise

    def change_columns_type(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Cast date and time columns"""
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

    def rename_columns(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Rename LT22 columns"""
        self.log.info("Renaming columns according to map")
        try:
            rename_map = {
                "N° OT": "num_ot",
                "Material": "partnumber",
                "Hora": "request_hour",
                "Dt.criação": "request_date",
                "Tp._duplicated_0": "deposit_type",
                "PosiDest": "supply_area"
            }
            df_collected = df.collect()
            df = self._rename(df_collected, rename_map)
            self.log.info("Columns renamed successfully")
            return df.lazy()
        except Exception:
            self.log.error("Error renaming columns", exc_info=True)
            raise


class LT22ProcessService:
    """Service to process LT22 warehouse data and update stock"""

    def __init__(self, db: Session):
        self.db = db
        self.log = logger("requests_closure")
        self.pkmc_client = PKMC_Client()
        self.log.info("Initializing LT22ProcessService")

    def process_lt22_pipeline(self) -> pl.LazyFrame:
        """Full LT22 processing pipeline"""
        self.log.info("Starting LT22 processing pipeline")
        
        try:
            loader = DefineDataFrame()
            df = loader.create_lt22_df()

            cleaner = CleanDataFrame()
            df = cleaner.rename_columns(df)
            df = cleaner.clean_columns(df)
            df = cleaner.filter_values(df)
            df = cleaner.change_columns_type(df)

            self.log.info("LT22 pipeline completed")
            return df

        except Exception:
            self.log.error("Error in LT22 processing pipeline", exc_info=True)
            raise

    def update_lb_balance(self, df_lt22: pl.LazyFrame | pl.DataFrame):
        """Update PKMC lb_balance based on LT22 data via external API"""
        self.log.info("Starting lb_balance update from LT22 data")

        if isinstance(df_lt22, pl.LazyFrame):
            df_lt22 = df_lt22.collect()

        try:
            # Join with requests_made
            stmt_requests = select(
                RequestsMade.partnumber,
                RequestsMade.supply_area,
                RequestsMade.num_reg_circ,
                RequestsMade.qty_to_request,
                RequestsMade.takt,
                RequestsMade.rack,
            )
            rows_requests = self.db.execute(stmt_requests).mappings().all()
            df_requests = pl.DataFrame(rows_requests)

            df_join = df_requests.lazy().join(
                df_lt22.lazy(),
                on=["partnumber", "supply_area"],
                how="inner"
            )

            # Fetch PKMC from external API
            lf_pkmc = self.pkmc_client.get_all()
            self.log.info("PKMC fetched from external API")

            df_pkmc_join = lf_pkmc.join(
                df_join,
                on=["partnumber", "rack"],
                how="inner"
            )

            # Calculate totals
            df_totals = (
                df_join
                .group_by(["partnumber", "rack"])
                .agg(pl.col("qty_to_request").sum().alias("qty_to_request"))
            )

            # Final update dataset
            df_final = (
                df_pkmc_join
                .join(df_totals, on=["partnumber", "rack"], how="left")
                .with_columns(
                    (pl.col("lb_balance") + pl.col("qty_to_request").fill_null(0))
                    .alias("lb_balance")
                )
                .select(["partnumber", "supply_area", "lb_balance"])
                .collect()
            )

            # Update via external API
            rows = df_final.to_dicts()
            result = self.pkmc_client.update(rows)
            self.log.info(f"Updated {len(rows)} PKMC records via external API: {result}")

        except Exception:
            self.db.rollback()
            self.log.error("Error updating lb_balance", exc_info=True)
            raise

    def delete_requests_made(self, df_lt22: pl.LazyFrame | pl.DataFrame, batch_size: int = 10000):
        """Delete processed requests from requests_made table"""
        self.log.info("Starting cleanup of processed requests")

        if isinstance(df_lt22, pl.LazyFrame):
            df_lt22 = df_lt22.collect()

        try:
            # Get partnumbers and supply_areas from LT22
            df_lt22_clean = df_lt22.select(["partnumber", "supply_area"]).unique()

            # Delete matching records
            to_delete = df_lt22_clean.to_dicts()
            for record in to_delete:
                stmt = sql_delete(RequestsMade).where(
                    (RequestsMade.partnumber == record["partnumber"]) &
                    (RequestsMade.supply_area == record["supply_area"])
                )
                self.db.execute(stmt)

            self.db.commit()
            self.log.info(f"Deleted {len(to_delete)} records from requests_made")

        except Exception:
            self.db.rollback()
            self.log.error("Error deleting from requests_made", exc_info=True)
            raise
