from datetime import datetime
from helpers.log.logger import logger


class LT22_Session:
    def __init__(self, sap):
        self.log = logger("sap")
        self.log.info("Inicializando LT22_Session")
        self.sap = sap

    def open(self):
        self.log.info("Abrindo transação /nLT22 no SAP")

        try:
            session, _ = self.sap.run_transaction("/nLT22")
            self.log.info("Sessão LT22 aberta com sucesso")
            return session

        except Exception:
            self.log.error("Erro ao abrir transação /nLT22 no SAP", exc_info=True)
            raise



class LT22_Selectors:
    def __init__(self, session):
        self.log = logger("sap")
        self.log.info("Inicializando LT22_Selectors")

        self.session = session
        self.node = "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[1]/shell"
        self.take = "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[0]/shell"

    def expand(self):
        self.log.info("Expandindo nó 68 em LT22")

        try:
            self.session.findById(self.node).expandNode("         68")
            self.log.info("Nó expandido com sucesso")

        except Exception:
            self.log.error("Erro ao expandir nó em LT22", exc_info=True)
            raise

    def select(self):
        self.log.info("Selecionando nós 108 e 123 em LT22")

        try:
            self.session.findById(self.node).selectNode("        108")
            self.session.findById(self.node).selectNode("        123")
            self.log.info("Nós selecionados com sucesso")

        except Exception:
            self.log.error("Erro ao selecionar nós em LT22", exc_info=True)
            raise

    def top(self):
        self.log.info("Definindo topNode como 123 em LT22")

        try:
            self.session.findById(self.node).topNode = "        123"
            self.log.info("topNode definido com sucesso")

        except Exception:
            self.log.error("Erro ao definir topNode em LT22", exc_info=True)
            raise

    def take(self):
        self.log.info("Executando botão TAKE em LT22")

        try:
            self.session.findById(self.take).pressButton("TAKE")
            self.log.info("Botão TAKE acionado com sucesso")

        except Exception:
            self.log.error("Erro ao acionar botão TAKE em LT22", exc_info=True)
            raise



class LT22_Parameters:
    def __init__(self, session):
        self.log = logger("sap")
        self.log.info("Inicializando LT22_Parameters")

        self.session = session
        self.b01 = "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/ssubSUBSCREEN_CONTAINER:SAPLSSEL:1106/ctxt%%DYN002-LOW"

    def set_deposit(self):
        self.log.info("Definindo depósito ANC em LT22")

        try:
            self.session.findById("wnd[0]/usr/ctxtT3_LGNUM").Text = "ANC"
            self.session.findById("wnd[0]/tbar[1]/btn[16]").press()
            self.log.info("Depósito ANC configurado com sucesso")

        except Exception:
            self.log.error("Erro ao configurar depósito ANC em LT22", exc_info=True)
            raise

    def set_b01(self):
        self.log.info("Definindo filtro B01 para LT22")

        try:
            self.session.findById(self.b01).text = "B01"
            self.session.findById("wnd[0]").sendVKey(0)
            self.log.info("Filtro B01 definido com sucesso")

        except Exception:
            self.log.error("Erro ao definir filtro B01 em LT22", exc_info=True)
            raise

    def set_pending_only(self):
        self.log.info("Definindo filtro: apenas pendentes")

        try:
            self.session.findById("wnd[0]/usr/radT3_OFFTA").select()
            self.log.info("Filtro pendentes ativado com sucesso")

        except Exception:
            self.log.error("Erro ao ativar filtro pendentes em LT22", exc_info=True)
            raise

    def set_dates_today(self):
        today = datetime.now().strftime("%d.%m.%Y")
        self.log.info(f"Definindo datas inicial/final como {today}")

        try:
            self.session.findById("wnd[0]/usr/ctxtBDATU-LOW").Text = today
            self.session.findById("wnd[0]/usr/ctxtBDATU-HIGH").Text = today
            self.log.info("Datas definidas com sucesso")

        except Exception:
            self.log.error("Erro ao definir datas em LT22", exc_info=True)
            raise

    def set_layout(self):
        self.log.info("Definindo layout /auto_line_feeding em LT22")

        try:
            self.session.findById("wnd[0]/usr/ctxtLISTV").Text = "/auto_line_feeding"
            self.log.info("Layout definido com sucesso")

        except Exception:
            self.log.error("Erro ao definir layout em LT22", exc_info=True)
            raise



class LT22_Submit:
    def __init__(self, session):
        self.log = logger("sap")
        self.log.info("Inicializando LT22_Submit")

        self.session = session

    def submit(self):
        self.log.info("Submetendo execução do LT22")

        try:
            self.session.findById("wnd[0]").sendVKey(9)
            self.session.findById("wnd[1]/usr/ctxtPRI_PARAMS-PDEST").text = "LOCL"
            self.session.findById("wnd[1]/tbar[0]/btn[13]").press()
            self.session.findById("wnd[1]/usr/btnSOFORT_PUSH").press()
            self.session.findById("wnd[1]/tbar[0]/btn[11]").press()

            self.log.info("LT22 submetido com sucesso")

        except Exception:
            self.log.error("Erro ao submeter LT22", exc_info=True)
            raise