from helpers.log.logger import logger


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