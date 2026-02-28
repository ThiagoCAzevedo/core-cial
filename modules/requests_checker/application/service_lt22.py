from sqlalchemy import select
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from database.models.requests_made import RequestsMade
from common.logger import logger
import os
from dotenv import load_dotenv


load_dotenv("config/.env")


class LT22Service:
    """Service to handle SAP LT22 warehouse transfer execution"""

    def __init__(self, sap, db: Session):
        self.sap = sap
        self.db = db
        self.log = logger("requests_checker")
        self.log.info("Initializing LT22Service")

    def open_lt22(self):
        """Open LT22 transaction in SAP"""
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
        """Execute LT22 request pipeline"""
        self.log.info("Starting LT22 pipeline")

        if not self.sap:
            raise Exception("No SAP session available")

        try:
            session = self.open_lt22()
        except Exception:
            self.log.error("Error opening LT22", exc_info=True)
            raise

        try:
            # Get requests data
            num_shipments = self._get_num_shipments()
            self.log.info(f"Found {len(num_shipments)} shipment(s) to process.")

            # Configure parameters
            params = LT22_Parameters(session)
            params.set_deposit()
            params.set_b01()
            params.set_confirmed_only()
            params.set_dates_today()
            params.set_layout()

            # Submit request
            submit = LT22_Submit(session)
            submit.submit()

            self.log.info("LT22 pipeline completed successfully")
            return True

        except Exception:
            self.log.error("Error executing LT22 pipeline", exc_info=True)
            raise

    def _get_num_shipments(self):
        """Retrieve all shipment numbers from requests_made table"""
        try:
            stmt = select(RequestsMade.num_shipment)
            rows = self.db.execute(stmt).all()
            return [row[0] for row in rows if row[0]]
        except Exception:
            self.log.error("Error fetching shipment numbers", exc_info=True)
            raise


class LT22_Parameters:
    """Helper class to set LT22 transaction parameters in SAP"""

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
    """Helper class to submit LT22 requests"""

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


def get_lt22_storage_path() -> str:
    """Get full path for LT22 output file storage"""
    base = os.environ.get("USERPROFILE", os.path.expanduser("~"))
    return os.path.join(base, ".000 - Projetos", "auto-line-feeding", "backend", "core", "storage", "sap")
