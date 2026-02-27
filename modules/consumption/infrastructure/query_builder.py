from sqlalchemy import select
from modules.forecast.infrastructure.models import Forecast
from modules.assembly.infrastructure.models import Assembly
from modules.pkmc.infrastructure.models import PKMC


class ConsumptionQueryBuilder:
    @staticmethod
    def build():
        return (
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
                & (Forecast.takt == Assembly.takt),
            )
            .join(
                PKMC,
                PKMC.partnumber == Forecast.partnumber,
            )
        )