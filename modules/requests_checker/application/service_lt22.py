from sqlalchemy import select
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from database.models.requests_made import RequestsMade
from common.logger import logger
import os


class LT22Service:
    def __init__(self, sap, db: Session):
        self.sap = sap
        self.db = db
        self.log = logger("requests_checker")
        self.log.info("Initializing LT22Service")

    def open_lt22(self):
        self.log.info("Opening SAP transaction /nLT22")
        if not self.sap:
            raise Exception("No SAP session available")

        try:
            session, _ = self.sap.run_transaction("/nLT22")
            self.log.info("LT22 session opened successfully")
            return session
        except Exception:
            self.log.error("Error opening SAP transaction /nLT22", exc_info=True)
            raise

    def request_lt22(self) -> bool:
        self.log.info("Starting LT22 pipeline")

        if not self.sap:
            raise Exception("No SAP session available")

        try:
            session = self.open_lt22()
        except Exception:
            self.log.error("Error opening LT22", exc_info=True)
            raise

        try:
            mapping = self._get_shipments_with_partnumbers()

            self.log.info(f"Found {len(mapping)} shipments to process.")

            for shipment, partnumbers in mapping.items():

                if not partnumbers:
                    self.log.warning(f"No partnumbers found for shipment {shipment}")
                    continue

                self.log.info(f"Shipment {shipment}: {len(partnumbers)} partnumbers found")

                for partnumber in partnumbers:

                    self.log.info(f"Processing shipment={shipment} | partnumber={partnumber}")

                    params = LT22_Parameters(session)
                    params.set_deposit()
                    params.set_shipment(shipment)
                    params.set_partnumber(partnumber)
                    params.set_b01()
                    params.set_confirmed_only()
                    params.set_dates_today()
                    params.set_layout()

                    submit = LT22_Submit(session)
                    submit.submit()
                    submit.extract_lt22()

            self.log.info("LT22 pipeline completed successfully")
            return True

        except Exception:
            self.log.error("Error executing LT22 pipeline", exc_info=True)
            raise

    def _get_shipments_with_partnumbers(self):
        try:
            stmt = select(RequestsMade.num_shipment, RequestsMade.partnumber)
            rows = self.db.execute(stmt).all()

            mapping = {}

            for shipment, partnumber in rows:
                if shipment is None or partnumber is None:
                    continue

                if shipment not in mapping:
                    mapping[shipment] = []

                mapping[shipment].append(partnumber)

            return mapping

        except Exception:
            self.log.error("Error fetching shipment/partnumber mapping", exc_info=True)
            raise


class LT22_Parameters:

    def __init__(self, session):
        self.log = logger("requests_checker")
        self.log.info("Initializing LT22_Parameters")
        self.session = session

    def set_deposit(self):
        self.log.info("Setting deposit ANC in LT22")
        try:
            self.session.findById("wnd[0]/usr/ctxtT3_LGNUM").Text = "ANC"
            self.log.info("Deposit ANC set successfully")
        except Exception:
            self.log.error("Error setting deposit ANC in LT22", exc_info=True)
            raise

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
 
    def set_b01(self):
        self.log.info("Setting filter B01 for LT22")
        try:
            self.session.findById("wnd[0]/usr/ctxtT3_LGTYP-HIGH").Text = "B01"
            self.session.findById("wnd[0]").sendVKey(0)
            self.log.info("Filter B01 set successfully")
        except Exception:
            self.log.error("Error setting B01 filter in LT22", exc_info=True)
            raise

    def set_confirmed_only(self):
        self.log.info("Setting filter: confirmed only")
        try:
            self.session.findById("wnd[0]/usr/radT3_QUITA").select()
            self.log.info("Confirmed filter activated successfully")
        except Exception:
            self.log.error("Error activating confirmed filter in LT22", exc_info=True)
            raise

    def set_dates_today(self):
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        date_today = today.strftime("%d.%m.%Y")
        date_yesterday = yesterday.strftime("%d.%m.%Y")

        self.log.info(f"Setting dates LOW={date_yesterday} | HIGH={date_today}")
        try:
            self.session.findById("wnd[0]/usr/ctxtBDATU-LOW").Text = date_yesterday
            self.session.findById("wnd[0]/usr/ctxtBDATU-HIGH").Text = date_today
            self.log.info("Dates set successfully")
        except Exception:
            self.log.error("Error setting dates in LT22", exc_info=True)
            raise

    def set_layout(self):
        self.log.info("Setting layout /auto-feed in LT22")
        try:
            self.session.findById("wnd[0]/usr/ctxtLISTV").Text = "/auto-feed"
            self.log.info("Layout set successfully")
        except Exception:
            self.log.error("Error setting layout in LT22", exc_info=True)
            raise


class LT22_Submit:
    def __init__(self, session):
        self.log = logger("requests_checker")
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

    def extract_lt22(self):
        self.session.findById("wnd[0]/tbar[1]/btn[9]").press()
        self.session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]").select()
        self.session.findById("wnd[1]/tbar[0]/btn[0]").press()
        self.session.findById("wnd[1]/usr/ctxtDY_PATH").text = os.path.join(os.environ["USERPROFILE"], ".000 - Projetos", "auto-line-feeding", "backend", "core", "storage", "sap")
        self.session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "lt22.txt"
        self.session.findById("wnd[0]/tbar[1]/btn[9]").press()
        self.session.findById("wnd[1]/tbar[0]/btn[0]").press()