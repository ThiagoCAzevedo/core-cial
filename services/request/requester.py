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


class LM01_Requester:
    def __init__(self, sap, df):
        self.log = logger("request")
        self.log.info("Inicializando LM01_Requester")

        self.sap = sap

        try:
            self.df = df.collect()
            self.log.info(f"DataFrame carregado no LM01_Requester — total de linhas: {self.df.height()}")

        except Exception:
            self.log.error("Erro ao coletar DataFrame em LM01_Requester", exc_info=True)
            raise


    def _request_lm01(self):
        self.log.info("Iniciando processo de requisição SAP LM01")

        try:
            session, _ = self.sap.run_transaction("/nLM01")
            self.log.info("Sessão SAP LM01 iniciada com sucesso")

        except Exception:
            self.log.error("Erro ao iniciar transação SAP /nLM01", exc_info=True)
            raise

        try:
            session.findById("wnd[0]/usr/txtGV_OT").setFocus()
            session.findById("wnd[0]/usr/btnTEXT1").press()
        except Exception:
            self.log.error("Erro ao preparar interface SAP LM01", exc_info=True)
            raise

        rows_requested = 0

        try:
            for row in self.df.iter_rows(named=True):
                qtd_caixas = int(row["qty_boxes_to_request"])
                num_circ = str(row["num_reg_circ"])

                self.log.info(f"Processando partnumber={row['partnumber']} | caixas={qtd_caixas}")

                for _ in range(qtd_caixas):
                    rows_requested += 1

                    try:
                        session.findById("wnd[0]/usr/ctxtVG_PKNUM").Text = num_circ
                        session.findById("wnd[0]").sendVKey(0)
                        session.findById("wnd[0]").sendVKey(8)
                        session.findById("wnd[0]/usr/btnRLMOB-POK").press()
                        session.findById("wnd[0]/usr/btnBTOK").press()

                    except Exception:
                        self.log.error(f"Erro ao solicitar caixa para num_reg_circ={num_circ}", exc_info=True)
                        raise

            self.log.info(f"Processo LM01 finalizado — total de linhas solicitadas: {rows_requested}")

        except Exception:
            self.log.error("Erro no loop de requisições LM01", exc_info=True)
            raise

        return rows_requested