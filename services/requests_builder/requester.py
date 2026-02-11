from helpers.log.logger import logger


class LM01_Requester:
    def __init__(self, sap, df):
        self.log = logger("request")
        self.log.info("Initializing LM01_Requester")

        self.sap = sap

        try:
            self.df = df.collect()
            self.log.info(f"DataFrame loaded into LM01_Requester — total rows: {self.df.height()}")

        except Exception:
            self.log.error("Error collecting DataFrame in LM01_Requester", exc_info=True)
            raise


    def _request_lm01(self):
        self.log.info("Starting SAP LM01 request process")

        try:
            session, _ = self.sap.run_transaction("/nLM01")
            self.log.info("SAP LM01 session started successfully")

        except Exception:
            self.log.error("Error starting SAP transaction /nLM01", exc_info=True)
            raise

        try:
            session.findById("wnd[0]/usr/txtGV_OT").setFocus()
            session.findById("wnd[0]/usr/btnTEXT1").press()
        except Exception:
            self.log.error("Error preparing SAP LM01 interface", exc_info=True)
            raise

        rows_requested = 0

        try:
            for row in self.df.iter_rows(named=True):
                qtd_caixas = int(row["qty_boxes_to_request"])
                num_circ = str(row["num_reg_circ"])

                self.log.info(f"Processing partnumber={row['partnumber']} | boxes={qtd_caixas}")

                for _ in range(qtd_caixas):
                    rows_requested += 1

                    try:
                        session.findById("wnd[0]/usr/ctxtVG_PKNUM").Text = num_circ
                        session.findById("wnd[0]").sendVKey(0)
                        session.findById("wnd[0]").sendVKey(8)
                        session.findById("wnd[0]/usr/btnRLMOB-POK").press()
                        session.findById("wnd[0]/usr/btnBTOK").press()

                    except Exception:
                        self.log.error(f"Error requesting box for num_reg_circ={num_circ}", exc_info=True)
                        raise

            self.log.info(f"LM01 process completed — total requested rows: {rows_requested}")

        except Exception:
            self.log.error("Error in LM01 request loop", exc_info=True)
            raise

        return rows_requested