from sqlalchemy import select
from common.logger import logger
from modules.consumption.infrastructure.models import Forecast, Assembly, PKMC
from modules.consumption.infrastructure.selectors import ConsumptionSelector
from modules.consumption.infrastructure.repository import ConsumptionUpdater
import polars as pl


class ConsumptionService:
    def __init__(self, db):
        self.log = logger("consumption")
        self.selector = ConsumptionSelector(db)
        self.updater = ConsumptionUpdater(db)

    def values_to_consume(self):
        self.log.info("Building SQL query for consumption values")

        stmt = (
            select(
                Forecast.partnumber, Forecast.takt, Forecast.rack, Forecast.knr_fx4pd,
                Forecast.qty_usage,
                Assembly.takt.label("assembly_takt"),
                PKMC.partnumber.label("pkmc_partnumber"),
                PKMC.lb_balance,
            )
            .join(
                Assembly,
                (Forecast.knr_fx4pd == Assembly.knr_fx4pd)
                & (Forecast.takt == Assembly.takt)
            )
            .join(PKMC, PKMC.partnumber == Forecast.partnumber)
        )

        lf = self.selector.select(stmt)

        lf = (
            lf.with_columns(
                (pl.col("lb_balance") - pl.col("qty_usage").fill_null(0))
                .alias("lb_balance")
            )
            .select(["partnumber", "lb_balance"])
        )

        return lf.collect()

    def update_infos(self, df: pl.DataFrame, batch_size: int):
        return self.updater.update_df(
            table_name="pkmc",
            df=df,
            key_column="partnumber",
            batch_size=batch_size,
        )