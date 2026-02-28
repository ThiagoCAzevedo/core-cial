from sqlalchemy import select
from sqlalchemy.orm import Session
from database.models.forecast import FX4PD
from modules.external_clients.pkmc_client import PKMC_Client
from modules.external_clients.pk05_client import PK05_Client
from common.logger import logger
import polars as pl


class ForecastService:

    def __init__(self, db: Session):
        self.db = db
        self.log = logger("forecast")
        self.pkmc_client = PKMC_Client()
        self.pk05_client = PK05_Client()
        self.log.info("Initializing ForecastService")

    def join_fx4pd_pkmc_pk05(self) -> pl.LazyFrame:
        self.log.info("Building join: FX4PD (local DB) + PKMC (external API) + PK05 (external API)")

        try:
            stmt = select(FX4PD)
            rows_fx4pd = self.db.execute(stmt).mappings().all()
            df_fx4pd = pl.DataFrame(rows_fx4pd).lazy()
            self.log.info(f"FX4PD loaded: {len(rows_fx4pd)} records")

            # Fetch PKMC and PK05 from external APIs
            lf_pkmc = self.pkmc_client.get_all()
            lf_pk05 = self.pk05_client.get_all()
            self.log.info("PKMC and PK05 fetched from external APIs")

            # Perform joins
            lf = (
                lf_pkmc
                .join(lf_pk05, on="supply_area", how="inner")
                .join(df_fx4pd, on="partnumber", how="inner")
                .select([
                    "num_reg_circ",
                    "takt",
                    "rack",
                    "lb_balance",
                    "partnumber",
                    "total_theoretical_qty",
                    "qty_for_restock",
                    "qty_per_box",
                    "qty_max_box",
                    "knr_fx4pd",
                    "qty_usage",
                    "qty_unit",
                ])
            )

            self.log.info("Join completed successfully")
            return lf

        except Exception:
            self.log.error("Error joining FX4PD + PKMC + PK05", exc_info=True)
            raise