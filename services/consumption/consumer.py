from sqlalchemy import select
from database.models.forecast import Forecast
from database.models.assembly import Assembly
from database.models.pkmc import PKMC
from database.queries import SelectInfos, UpdateInfos
import polars as pl
    

class ConsumeValues(SelectInfos):
    def __init__(self, db):
        SelectInfos.__init__(self, db)

    def values_to_consume(self):
        stmt = (
            select(
                Forecast.partnumber,
                Forecast.takt,
                Forecast.rack,
                Forecast.knr_fx4pd,
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
            .join(
                PKMC,
                PKMC.partnumber == Forecast.partnumber
            )
        )

        df = self.select(stmt)

        return (
            df.with_columns(
                (pl.col("lb_balance") - pl.col("qty_usage").fill_null(0))
                .alias("lb_balance")
            )
            .select(["partnumber", "lb_balance"])
            .collect()
        )

    def _update_infos(self, df, batch_size):
        update = UpdateInfos(self.db)
        update.update_df(
            table_name="pkmc",
            df=df,
            key_column="partnumber",
            batch_size=batch_size
        )
