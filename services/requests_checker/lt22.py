from datetime import datetime, timedelta
from helpers.log.logger import logger
from database.queries import SelectInfos
from database.models.requests_made import RequestsMade
from sqlalchemy import select


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


class ReturnValuesRequestsMade:
    def __init__(self, db):
        self.selector = SelectInfos(db)

    def get_all_num_shipment(self):
        stmt = (
            select(
                RequestsMade.num_shipment
                )
            )

        lf = self.selector.select(stmt)
        df = lf.collect()             

        return [row["num_shipment"] for row in df.iter_rows(named=True)]
    
    def get_all_partnumber(self):
        stmt = (
            select(
                RequestsMade.partnumber
                )
            )

        lf = self.selector.select(stmt)
        df = lf.collect()             

        return [row["partnumber"] for row in df.iter_rows(named=True)]
    

class LT22_Parameters:
    def __init__(self, session):
        self.log = logger("sap")
        self.log.info("Initializing LT22_Parameters")

        self.session = session

    def set_shipment(self, num_shipment):
        self.session.findById("wnd[0]/tbar[1]/btn[16]").press()
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[1]/shell").expandNode("         68")
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[1]/shell").selectNode("        218")
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[1]/shell").topNode = "        212"
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[1]/shell").doubleClickNode("        218")
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/ssubSUBSCREEN_CONTAINER:SAPLSSEL:1106/txt%%DYN001-LOW").text = num_shipment
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/ssubSUBSCREEN_CONTAINER:SAPLSSEL:1106/txt%%DYN001-LOW").setFocus()
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/ssubSUBSCREEN_CONTAINER:SAPLSSEL:1106/txt%%DYN001-LOW").caretPosition = 9
        self.session.findById("wnd[0]").sendVKey(0)

    def set_partnumber(self, partnumber):
        self.session.findById("wnd[0]/tbar[1]/btn[16]").press
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[1]/shell").expandNode("         68")
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[1]/shell").selectNode("         74")
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[1]/shell").topNode = "         71"
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[1]/shell").doubleClickNode("         74")
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/ssubSUBSCREEN_CONTAINER:SAPLSSEL:1106/ctxt%%DYN001-LOW").text = partnumber
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/ssubSUBSCREEN_CONTAINER:SAPLSSEL:1106/ctxt%%DYN001-LOW").setFocus()
        self.session.findById("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/ssubSUBSCREEN_CONTAINER:SAPLSSEL:1106/ctxt%%DYN001-LOW").caretPosition = 18
        self.session.findById("wnd[0]").sendVKey(0)

    def set_deposit(self):
        self.log.info("Setting deposit ANC in LT22")

        try:
            self.session.findById("wnd[0]/usr/ctxtT3_LGNUM").Text = "ANC"
            self.log.info("Deposit ANC set successfully")

        except Exception:
            self.log.error("Error setting deposit ANC in LT22", exc_info=True)
            raise

    def set_b01(self):
        self.log.info("Setting filter B01 for LT22")

        try:
            self.session.findById("wnd[0]/usr/ctxtT3_LGTYP-HIGH").text = "B01"
            self.session.findById("wnd[0]").sendVKey(0)
            self.log.info("Filter B01 set successfully")

        except Exception:
            self.log.error("Error setting B01 filter in LT22", exc_info=True)
            raise

    def set_confirmed_only(self):
        self.log.info("Setting filter: pending only")

        try:
            self.session.findById("wnd[0]/usr/radT3_QUITA").select()
            self.log.info("Pending filter activated successfully")

        except Exception:
            self.log.error("Error activating pending filter in LT22", exc_info=True)
            raise

    def set_dates_today(self):
        today = datetime.now()
        yesterday = today - timedelta(days=1)

        date_today = today.strftime("%d.%m.%Y")
        date_yesterday = yesterday.strftime("%d.%m.%Y")

        self.log.info(f"Setting date LOW={date_yesterday} | HIGH={date_today}")

        try:
            self.session.findById("wnd[0]/usr/ctxtBDATU-LOW").Text = date_yesterday
            self.session.findById("wnd[0]/usr/ctxtBDATU-HIGH").Text = date_today

            self.log.info("Dates set successfully")

        except Exception:
            self.log.error("Error setting dates in LT22", exc_info=True)
            raise

    def set_layout(self):
        self.log.info("Setting layout /auto_line_feeding in LT22")

        try:
            self.session.findById("wnd[0]/usr/ctxtLISTV").text = "/auto-feed"
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
            self.session.findById("wnd[0]").sendVKey(8)
            self.log.info("LT22 submitted successfully")

        except Exception:
            self.log.error("Error submitting LT22", exc_info=True)
            raise