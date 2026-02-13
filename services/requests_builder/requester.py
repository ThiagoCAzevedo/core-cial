from helpers.log.logger import logger
from database.queries import UpdateInfos
import polars as pl
import re


class LM01_Requester:
    def __init__(self, sap, df, db_session):
        self.log = logger("request")
        self.log.info("Initializing LM01_Requester")

        self.sap = sap
        self.db = db_session

        try:
            self.df = df.collect()
            self.log.info(f"DataFrame loaded — rows: {self.df.height()}")

        except Exception:
            self.log.error("Error collecting DataFrame", exc_info=True)
            raise


    def _request_lm01(self):
        self.log.info("Starting SAP LM01 request")

        try:
            session, _ = self.sap.run_transaction("/nLM01")
            self.log.info("SAP LM01 session OK")

        except Exception:
            self.log.error("Error starting SAP LM01", exc_info=True)
            raise

        try:
            session.findById("wnd[0]/usr/txtGV_OT").setFocus()
            session.findById("wnd[0]/usr/btnTEXT1").press()
        except Exception:
            self.log.error("Error preparing LM01 UI", exc_info=True)
            raise

        ot_reader = LM01_ReturnNumOT(session)
        updater = UpdateInfos(self.db)

        rows_requested = 0

        try:
            for row in self.df.iter_rows(named=True):

                qtd_caixas = int(row["qty_boxes_to_request"])
                num_circ = str(row["num_reg_circ"])

                self.log.info(f"Processing part={row['partnumber']} | caixas={qtd_caixas}")

                for _ in range(qtd_caixas):

                    rows_requested += 1

                    try:
                        session.findById("wnd[0]/usr/ctxtVG_PKNUM").Text = num_circ
                        session.findById("wnd[0]").sendVKey(0)
                        session.findById("wnd[0]").sendVKey(8)
                        session.findById("wnd[0]/usr/btnRLMOB-POK").press()
                        session.findById("wnd[0]/usr/btnBTOK").press()

                    except Exception:
                        self.log.error(f"Erro solicitando caixa num_circ={num_circ}", exc_info=True)
                        raise

                    num_ot = ot_reader.get_ot_number()

                    if num_ot:
                        df_update = pl.DataFrame({
                            "num_reg_circ": [num_circ],
                            "num_ot": [num_ot]
                        })

                        updater.update_df(
                            table_name="requests_made",
                            df=df_update,
                            key_column="num_reg_circ"
                        )

                    else:
                        self.log.warning(f"Nenhuma OT retornada p/ {num_circ}")

            self.log.info(f"LM01 finalizado — total caixas: {rows_requested}")

        except Exception:
            self.log.error("Erro no loop LM01", exc_info=True)
            raise

        return rows_requested
    

class LM01_ReturnNumOT:
    def __init__(self, session):
        self.session = session
        self.log = logger("return_ot")

    def get_ot_number(self):
        try:
            msg = self.session.findById("wnd[0]/sbar").Text
            match = re.search(r"(\d+)", msg)

            if match:
                ot = match.group(1)
                self.log.info(f"OT capturada: {ot}")
                return ot

            self.log.warning(f"Nenhuma OT encontrada na mensagem: {msg}")
            return None

        except Exception:
            self.log.error("Erro ao capturar OT", exc_info=True)
            return None