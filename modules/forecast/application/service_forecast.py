from sqlalchemy import select
from modules.forecast.infrastructure.models import FX4PD
from modules.external_clients.pkmc_client import PKMC_Client
from modules.external_clients.pk05_client import PK05_Client
from common.logger import logger
import polars as pl


class ForecastService:
    def __init__(self, db, pkmc_client: PKMC_Client, pk05_client: PK05_Client, fx4pd_repo):
        self.db = db
        self.pkmc_client = pkmc_client
        self.pk05_client = pk05_client
        self.fx4pd_repo = fx4pd_repo
        self.log = logger("forecast")

    def join_fx4pd_pkmc_pk05(self):
        lf_pkmc = self.pkmc_client.get_all()
        lf_pk05 = self.pk05_client.get_all()

        df_fx4pd = pl.DataFrame(
            self.db.execute(select(FX4PD)).mappings().all()
        )
        lf_fx4pd = df_fx4pd.lazy()

        lf = (
            lf_pkmc
            .join(lf_pk05, on="supply_area", how="inner")
            .join(lf_fx4pd, on="partnumber", how="inner")
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

        return lf