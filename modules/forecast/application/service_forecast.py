from sqlalchemy import select
from modules.forecast.infrastructure.models import FX4PD
from modules.pkmc.infrastructure.models import PKMC
from modules.pk05.infrastructure.models import PK05
from common.logger import logger
import polars as pl


class ForecastService:
    def __init__(self, db):
        self.db = db
        self.log = logger("forecast")

    def join_fx4pd_pkmc_pk05(self):
        stmt = (
            select(
                PKMC.num_reg_circ,
                PK05.takt,
                PKMC.rack,
                PKMC.lb_balance,
                PKMC.partnumber,
                PKMC.total_theoretical_qty,
                PKMC.qty_for_restock,
                PKMC.qty_per_box,
                PKMC.qty_max_box,
                FX4PD.knr_fx4pd,
                FX4PD.qty_usage,
                FX4PD.qty_unit,
            )
            .select_from(PKMC)
            .join(PK05, PK05.supply_area == PKMC.supply_area)
            .join(FX4PD, FX4PD.partnumber == PKMC.partnumber)
        )

        rows = self.db.execute(stmt).all()
        return pl.DataFrame(rows).lazy()