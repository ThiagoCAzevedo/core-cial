from database.queries import SelectInfos
from database.models.pkmc import PKMC
from database.models.pk05 import PK05
from sqlalchemy import select
from helpers.log.logger import logger
import polars as pl


class QuantityToRequest(SelectInfos):
    def __init__(self):
        self.log = logger("request")
        self.log.info("Inicializando QuantityToRequest")

        SelectInfos.__init__(self)

    def _define_diference_to_request(self):
        self.log.info("Montando query de diferença para solicitação (PKMC x PK05)")

        try:
            stmt = (
                select(
                    PKMC.partnumber, PKMC.num_reg_circ, PK05.takt, PKMC.rack,
                    PKMC.lb_balance, PKMC.total_theoretical_qty, PKMC.qty_for_restock, PKMC.qty_per_box,
                    PKMC.qty_max_box,
                )
                .join(PK05, PK05.supply_area == PKMC.supply_area)
                .where(PKMC.lb_balance <= PKMC.qty_for_restock)
            )
            self.log.info("Query SQL construída com sucesso")

        except Exception:
            self.log.error("Erro ao montar query SQL em QuantityToRequest", exc_info=True)
            raise

        try:
            df = self.select(stmt)
            self.log.info(f"Select concluído — registros retornados: {df.height()}")

        except Exception:
            self.log.error("Erro ao executar SELECT na função _define_diference_to_request", exc_info=True)
            raise

        try:
            self.log.info("Calculando qty_to_request e qty_boxes_to_request")

            df = df.with_columns([
                (pl.col("total_theoretical_qty") - pl.col("lb_balance"))
                    .alias("qty_to_request"),

                ((pl.col("total_theoretical_qty") - pl.col("lb_balance")) / pl.col("qty_per_box"))
                    .floor()
                    .alias("qty_boxes_to_request")
            ])

            df = df.select(["partnumber", "num_reg_circ", "qty_to_request",
                            "qty_boxes_to_request", "takt", "rack"])

            self.log.info("Cálculos concluídos com sucesso")

            return df

        except Exception:
            self.log.error("Erro durante cálculo das colunas qty_to_request / qty_boxes_to_request", exc_info=True)
            raise