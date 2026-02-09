from database.queries import SelectInfos
from sqlalchemy import select
from database.models.fx4pd import FX4PD
from database.models.pkmc import PKMC
from database.models.pk05 import PK05


class DefineForecastValues(SelectInfos):
    def join_fx4pd_pkmc_pk05(self):
        stmt = (
            select(
                FX4PD.knr_fx4pd,
                FX4PD.partnumber,
                FX4PD.qty_usage,
                FX4PD.qty_unit,
                PKMC.num_reg_circ,
                PK05.takt,
                PKMC.rack,
                PKMC.lb_balance,
                PKMC.total_theoretical_qty,
                PKMC.qty_for_restock,
                PKMC.qty_per_box,
                PKMC.qty_max_box,
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

        return self.select(stmt)