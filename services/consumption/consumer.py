from sqlalchemy import select
from database.models.forecast import Forecast
from database.models.assembly import Assembly
from database.models.pkmc import PKMC
from database.queries import SelectInfos, UpdateInfos
from helpers.log.logger import logger
import polars as pl
    

class ConsumeValues:
    def __init__(self, db):
        self.log = logger("consumption")
        self.selector = SelectInfos(db)
        self.updater = UpdateInfos(db)

        self.log.info("Initializing ConsumeValues")

    def values_to_consume(self):
        self.log.info("Building query to retrieve consumption values (Forecast x Assembly x PKMC)")

        try:
            stmt = (
                select(
                    Forecast.partnumber, Forecast.takt, Forecast.rack, Forecast.knr_fx4pd,
                    Forecast.qty_usage, Assembly.takt.label("assembly_takt"), PKMC.partnumber.label("pkmc_partnumber"), PKMC.lb_balance
                )
                .join(
                    Assembly,
                    (Forecast.knr_fx4pd == Assembly.knr_fx4pd)
                    & (Forecast.takt == Assembly.takt)
                )
                .join(
                    PKMC, PKMC.partnumber == Forecast.partnumber
                )
            )
            self.log.info("SQL query successfully built")

        except Exception:
            self.log.error("Error building SQL query", exc_info=True)
            raise

        try:
            lf = self.selector.select(stmt)
            self.log.info(f"Select completed — records returned: {lf.select(pl.len()).collect().item()}")

        except Exception:
            self.log.error("Error executing SELECT on database", exc_info=True)
            raise

        try:
            self.log.info("Calculating updated lb_balance based on qty_usage")

            lf = (
                lf.with_columns(
                    (pl.col("lb_balance") - pl.col("qty_usage").fill_null(0))
                    .alias("lb_balance")
                )
                .select(["partnumber", "lb_balance"])
                .collect()
            )

            self.log.info("Calculation completed — final DataFrame prepared")

        except Exception:
            self.log.error("Error calculating lb_balance in DataFrame", exc_info=True)
            raise

        return lf.collect()

    def _update_infos(self, df: pl.DataFrame, batch_size: int):
        self.log.info(f"Starting update on PKMC table for {df.height()} records")

        try:
            self.updater.update_df(
                table_name="pkmc",
                df=df,
                key_column="partnumber",
                batch_size=batch_size
            )

            self.log.info("Update completed successfully")

        except Exception:
            self.log.error("Error executing update on database", exc_info=True)
            raise