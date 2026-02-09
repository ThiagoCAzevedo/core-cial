from database.queries import SelectInfos
from database.models.pkmc import PKMC
from database.models.pk05 import PK05
from sqlalchemy import select
import polars as pl


class QuantityToRequest(SelectInfos):
    def __init__(self):
        SelectInfos.__init__(self)

    def _define_diference_to_request(self):
        stmt = (
            select(
                PKMC.partnumber, PKMC.num_reg_circ, PK05.takt, PKMC.rack,
                PKMC.lb_balance, PKMC.total_theoretical_qty, PKMC.qty_for_restock, PKMC.qty_per_box,
                PKMC.qty_max_box,
            )
            .join(PK05, PK05.supply_area == PKMC.supply_area)
            .where(PKMC.lb_balance <= PKMC.qty_for_restock)
        )

        df = self.select(stmt)
        df = df.with_columns([
            (pl.col("total_theoretical_qty") - pl.col("lb_balance"))
                .alias("qty_to_request"),

            ((pl.col("total_theoretical_qty") - pl.col("lb_balance")) / pl.col("qty_per_box"))
                .floor()
                .alias("qty_boxes_to_request")
        ])

        return df.select(["partnumber", "num_reg_circ", "qty_to_request", "qty_boxes_to_request", "takt", "rack"])
    

class LM01_Requester:
    def __init__(self, sap, df):
        self.sap = sap
        self.df = df.collect()

    def _request_lm01(self):
        session, _ = self.sap.run_transaction("/nLM01")

        session.findById("wnd[0]/usr/txtGV_OT").setFocus()
        session.findById("wnd[0]/usr/btnTEXT1").press()
        rows_requested = 0

        for row in self.df.iter_rows(named=True):
            qtd_caixas = int(row["qty_boxes_to_request"])
            num_circ = str(row["num_reg_circ"])

            for _ in range(qtd_caixas):
                rows_requested+=1
                session.findById("wnd[0]/usr/ctxtVG_PKNUM").Text = num_circ
                session.findById("wnd[0]").sendVKey(0)
                session.findById("wnd[0]").sendVKey(8)
                session.findById("wnd[0]/usr/btnRLMOB-POK").press()
                session.findById("wnd[0]/usr/btnBTOK").press()

        return rows_requested