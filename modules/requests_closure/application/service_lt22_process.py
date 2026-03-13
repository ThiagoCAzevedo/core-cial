from common.logger import logger
from sqlalchemy import select, delete as sql_delete
from sqlalchemy.orm import Session
from modules.requests_builder.domain.models import RequestsMade
from modules.requests_closure.infrastructure.pkmc_adapter import PKMC_Client
from modules.requests_closure.application.lt22_data_loader import DefineDataFrame
from modules.requests_closure.application.lt22_data_cleaner import CleanDataFrame
import polars as pl


class LT22ProcessService:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("requests_closure")
        self.pkmc_client = PKMC_Client()
        self.log.info("Initializing LT22ProcessService")

    def process_lt22_pipeline(self) -> pl.LazyFrame:
        self.log.info("Starting LT22 processing pipeline")
        
        try:
            loader = DefineDataFrame()
            df = loader.create_lt22_df()

            cleaner = CleanDataFrame()
            df = cleaner.rename_columns(df)
            df = cleaner.clean_columns(df)
            df = cleaner.change_columns_type(df)
            self.update_lb_balance(df)
            self.delete_requests_made(df)

            self.log.info("LT22 pipeline completed")
            return df

        except Exception:
            self.log.error("Error in LT22 processing pipeline", exc_info=True)
            raise

    def update_lb_balance(self, df_lt22: pl.LazyFrame | pl.DataFrame):
        self.log.info("Starting lb_balance update from LT22 data")

        if isinstance(df_lt22, pl.LazyFrame):
            df_lt22 = df_lt22.collect()

        try:
            stmt_requests = select(
                RequestsMade.partnumber,
                RequestsMade.supply_area,
                RequestsMade.num_reg_circ,
                RequestsMade.qty_to_request,
                RequestsMade.takt,
                RequestsMade.rack,
            )

            rows_requests = self.db.execute(stmt_requests).mappings().all()
            rows_requests = [dict(r) for r in rows_requests]

            df_requests = pl.from_dicts(rows_requests)

            df_join = df_requests.lazy().join(
                df_lt22.lazy(),
                on=["partnumber"],
                how="inner"
            )


            lf_pkmc = self.pkmc_client.get_all()
            self.log.info("PKMC fetched from external API")

            df_pkmc_join = lf_pkmc.join(
                df_join,
                on=["partnumber"],
                how="inner"
            )

            df_totals = (
                df_join
                .group_by(["partnumber"])
                .agg(pl.col("qty_to_request").sum().alias("qty_to_request"))
            )

            df_final = (
                df_pkmc_join
                .join(df_totals, on=["partnumber"], how="left")
                .with_columns(
                    (pl.col("lb_balance") + pl.col("qty_to_request").fill_null(0))
                    .alias("lb_balance")
                )
                .select(["partnumber", "supply_area", "lb_balance"])
                .collect()
            )

            rows = df_final.to_dicts()
            result = self.pkmc_client.update(rows)
            self.log.info(f"Updated {len(rows)} PKMC records via external API: {result}")

        except Exception:
            self.db.rollback()
            self.log.error("Error updating lb_balance", exc_info=True)
            raise

    def delete_requests_made(self, df_lt22: pl.LazyFrame | pl.DataFrame):
        self.log.info("Starting cleanup of processed requests")

        if isinstance(df_lt22, pl.LazyFrame):
            df_lt22 = df_lt22.collect()

        try:
            df_lt22_clean = df_lt22.select(["partnumber"]).unique()

            to_delete = df_lt22_clean.to_dicts()
            for record in to_delete:
                stmt = sql_delete(RequestsMade).where(
                    (RequestsMade.partnumber == record["partnumber"]) 
                )
                self.db.execute(stmt)

            self.db.commit()
            self.log.info(f"Deleted {len(to_delete)} records from requests_made")

        except Exception:
            self.db.rollback()
            self.log.error("Error deleting from requests_made", exc_info=True)
            raise
