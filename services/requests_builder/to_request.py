from database.queries import SelectInfos
from database.models.pkmc import PKMC
from database.models.pk05 import PK05
from sqlalchemy import select
import polars as pl
from helpers.log.logger import logger


class QuantityToRequest:
    def __init__(self, session):
        self.session = session
        self.selector = SelectInfos(session)

        self.log = logger("request")
        self.log.info("Initializing QuantityToRequest")

    def _define_diference_to_request(self):
        self.log.info("Building query for request difference")

        stmt = (
            select(
                PKMC.partnumber,
                PKMC.supply_area,
                PKMC.num_reg_circ,
                PK05.takt,
                PKMC.rack,
                PKMC.lb_balance,
                PKMC.total_theoretical_qty,
                PKMC.qty_for_restock,
                PKMC.qty_per_box,
                PKMC.qty_max_box,
            )
            .join(PK05, PK05.supply_area == PKMC.supply_area)
            .where(PKMC.lb_balance <= PKMC.qty_for_restock)
        )

        # Agora chamamos select com SELECT complexo
        df = self.selector.select(stmt)

        df = df.with_columns([
            (pl.col("total_theoretical_qty") - pl.col("lb_balance"))
                .alias("qty_to_request"),

            ((pl.col("total_theoretical_qty") - pl.col("lb_balance"))
                / pl.col("qty_per_box"))
                .floor()
                .alias("qty_boxes_to_request")
        ])

        df = df.select([
            "partnumber",
            "num_reg_circ",
            "qty_to_request",
            "qty_boxes_to_request",
            "takt",
            "rack"
        ])

        return df