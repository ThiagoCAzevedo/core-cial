from datetime import datetime
from helpers.log.logger import logger


class LT22_Session:
    def __init__(self, sap):
        self.log = logger("sap")
        self.log.info("Initializing LT22_Session")
        self.sap = sap

    def open(self):
        self.log.info("Opening SAP transaction /nLT22")

        try:
            session, _ = self.sap.run_transaction("/nLT22")
            self.log.info("LT22 session opened successfully")
            return session

        except Exception:
            self.log.error("Error opening SAP transaction /nLT22", exc_info=True)
            raise


class LT22_Selectors:
    def __init__(self, session):
        self.log = logger("sap")
        self.log.info("Initializing LT22_Selectors")

        self.session = session
        self.node = (
            "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/"
            "ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/"
            "shellcont/shell/shellcont[1]/shell"
        )
        self.take = (
            "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/"
            "ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/"
            "shellcont/shell/shellcont[0]/shell"
        )

    def expand(self):
        self.log.info("Expanding node 68 in LT22")

        try:
            self.session.findById(self.node).expandNode("         68")
            self.log.info("Node expanded successfully")

        except Exception:
            self.log.error("Error expanding node in LT22", exc_info=True)
            raise

    def select(self):
        self.log.info("Selecting nodes 108 and 123 in LT22")

        try:
            self.session.findById(self.node).selectNode("        108")
            self.session.findById(self.node).selectNode("        123")
            self.log.info("Nodes selected successfully")

        except Exception:
            self.log.error("Error selecting nodes in LT22", exc_info=True)
            raise

    def top(self):
        self.log.info("Setting topNode to 123 in LT22")

        try:
            self.session.findById(self.node).topNode = "        123"
            self.log.info("topNode set successfully")

        except Exception:
            self.log.error("Error setting topNode in LT22", exc_info=True)
            raise

    def take(self):
        self.log.info("Pressing TAKE button in LT22")

        try:
            self.session.findById(self.take).pressButton("TAKE")
            self.log.info("TAKE button pressed successfully")

        except Exception:
            self.log.error("Error pressing TAKE button in LT22", exc_info=True)
            raise


class LT22_Parameters:
    def __init__(self, session):
        self.log = logger("sap")
        self.log.info("Initializing LT22_Parameters")

        self.session = session
        self.b01 = (
            "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/"
            "ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/ssubSUBSCREEN_CONTAINER:"
            "SAPLSSEL:1106/ctxt%%DYN002-LOW"
        )

    def set_deposit(self):
        self.log.info("Setting deposit ANC in LT22")

        try:
            self.session.findById("wnd[0]/usr/ctxtT3_LGNUM").Text = "ANC"
            self.session.findById("wnd[0]/tbar[1]/btn[16]").press()
            self.log.info("Deposit ANC set successfully")

        except Exception:
            self.log.error("Error setting deposit ANC in LT22", exc_info=True)
            raise

    def set_b01(self):
        self.log.info("Setting filter B01 for LT22")

        try:
            self.session.findById(self.b01).text = "B01"
            self.session.findById("wnd[0]").sendVKey(0)
            self.log.info("Filter B01 set successfully")

        except Exception:
            self.log.error("Error setting B01 filter in LT22", exc_info=True)
            raise

    def set_pending_only(self):
        self.log.info("Setting filter: pending only")

        try:
            self.session.findById("wnd[0]/usr/radT3_OFFTA").select()
            self.log.info("Pending filter activated successfully")

        except Exception:
            self.log.error("Error activating pending filter in LT22", exc_info=True)
            raise

    def set_dates_today(self):
        today = datetime.now().strftime("%d.%m.%Y")
        self.log.info(f"Setting initial/final dates to {today}")

        try:
            self.session.findById("wnd[0]/usr/ctxtBDATU-LOW").Text = today
            self.session.findById("wnd[0]/usr/ctxtBDATU-HIGH").Text = today
            self.log.info("Dates set successfully")

        except Exception:
            self.log.error("Error setting dates in LT22", exc_info=True)
            raise

    def set_layout(self):
        self.log.info("Setting layout /auto_line_feeding in LT22")

        try:
            self.session.findById("wnd[0]/usr/ctxtLISTV").Text = "/auto_line_feeding"
            self.log.info("Layout set successfully")

        except Exception:
            self.log.error("Error setting layout in LT22", exc_info=True)
            raise


class LT22_Submit:
    def __init__(self, session):
        self.log = logger("sap")
        self.log.info("Initializing LT22_Submit")

        self.session = session

    def submit(self):
        self.log.info("Submitting LT22 execution")

        try:
            self.session.findById("wnd[0]").sendVKey(9)
            self.session.findById("wnd[1]/usr/ctxtPRI_PARAMS-PDEST").text = "LOCL"
            self.session.findById("wnd[1]/tbar[0]/btn[13]").press()
            self.session.findById("wnd[1]/usr/btnSOFORT_PUSH").press()
            self.session.findById("wnd[1]/tbar[0]/btn[11]").press()

            self.log.info("LT22 submitted successfully")

        except Exception:
            self.log.error("Error submitting LT22", exc_info=True)
            raise