from common.logger import logger
from config.settings import settings


class SAP_Authenticator:
    def __init__(self):
        self.log = logger("sap_manager")
        self.log.info("Initializing SAP_Authenticator")
        self.user = settings.SAP_USER
        self.password = settings.SAP_PSWD

    def login(self, session):
        self.log.info("Performing SAP login")
        try:
            session.findById("wnd[0]/usr/txtRSYST-BNAME").Text = self.user
            session.findById("wnd[0]/usr/pwdRSYST-BCODE").Text = self.password
            session.findById("wnd[0]").sendVKey(0)
            self.log.info("SAP login submitted successfully")

        except Exception:
            self.log.error("Error during SAP login", exc_info=True)
            raise
