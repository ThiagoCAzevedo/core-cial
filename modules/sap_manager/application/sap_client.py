from typing import Tuple
from common.logger import logger
import pythoncom
from modules.sap_manager.application.sap_launcher import SAP_Launcher
from modules.sap_manager.application.sap_session_provider import SAP_SessionProvider
from modules.sap_manager.application.sap_authenticator import SAP_Authenticator


class SAP_Client:
    def __init__(self):
        self.log = logger("sap_manager")
        self.log.info("Initializing SAP_Client")
        self.session_provider = SAP_SessionProvider()
        self.authenticator = SAP_Authenticator()
        self.launcher = SAP_Launcher()
        self.session = None
        self.already_opened = False

    def connect(self):
        self.log.info("Connecting to SAP...")
        try:
            pythoncom.CoInitialize()
            sess = self.session_provider.get_existing_session()
            if sess:
                self.session = sess
                self.already_opened = True
                self.log.info("Connected to existing SAP session")
                return self.session

            self.log.info("No active session found — launching new SAP instance")
            self.launcher.start()
            self.session = self.session_provider.open_new_session()
            self.authenticator.login(self.session)
            self.log.info("SAP connection established successfully")
            return self.session

        except Exception:
            self.log.error("Error connecting to SAP", exc_info=True)
            raise

    def run_transaction(self, tcode: str = "/n") -> Tuple[object, bool]:
        self.log.info(f"Executing SAP transaction: {tcode}")
        try:
            if not self.session:
                self.connect()

            self.session.findById("wnd[0]/tbar[0]/okcd").Text = tcode
            self.session.findById("wnd[0]").sendVKey(0)
            self.log.info(f"Transaction {tcode} executed successfully")
            return self.session, self.already_opened

        except Exception:
            self.log.error(f"Error executing SAP transaction: {tcode}", exc_info=True)
            raise
