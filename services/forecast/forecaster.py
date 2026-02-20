from database.queries import SelectInfos
from sqlalchemy import select
from database.models.fx4pd import FX4PD
from database.models.pkmc import PKMC
from database.models.pk05 import PK05
from helpers.log.logger import logger
import polars as pl


class DefineForecastValues(SelectInfos):
    def __init__(self, db):
        self.log = logger("forecast")
        self.log.info("Initializing DefineForecastValues")
        self.selector = SelectInfos(db)

    def join_fx4pd_pkmc_pk05(self):
        self.log.info("Building join query: FX4PD + PKMC + PK05")

        try:
            stmt = (
                select(
                    FX4PD.knr_fx4pd, FX4PD.partnumber, FX4PD.qty_usage, FX4PD.qty_unit,
                    PKMC.num_reg_circ, PK05.takt, PKMC.rack, PKMC.lb_balance,
                    PKMC.total_theoretical_qty, PKMC.qty_for_restock, PKMC.qty_per_box, PKMC.qty_max_box,
                )
                .join(
                    PKMC,
                    PKMC.partnumber == FX4PD.partnumber
                )
                .join(
                    PK05,
                    PK05.supply_area == PKMC.supply_area
                )
            )

            self.log.info("SQL query successfully built")

        except Exception:
            self.log.error("Error building SQL query in join_fx4pd_pkmc_pk05", exc_info=True)
            raise

        try:
            df = self.selector.select(stmt)
            # self.log.info(f"Select completed — records returned: {df.height}")
            return df

        except Exception:
            self.log.error("Error executing SELECT in join_fx4pd_pkmc_pk05", exc_info=True)
            raise