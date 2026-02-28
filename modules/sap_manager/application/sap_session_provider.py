from common.logger import logger
from config.settings import settings
from modules.sap_manager.application.sap_launcher import SAP_Launcher


class SAP_SessionProvider:
    def __init__(self):
        self.log = logger("sap_manager")
        self.log.info("Initializing SAP_SessionProvider")
        self.launcher = SAP_Launcher()
        self.connection_name = settings.SAP_CONNECTION_NAME

    def get_existing_session(self):
        self.log.info("Searching for an existing SAP session")
        try:
            app = self.launcher.get_application()
            if not app:
                self.log.info("No active SAPGUI instance found")
                return None

            if app.Children.Count > 0:
                conn = app.Children(0)
                if conn.Children.Count > 0:
                    self.log.info("Existing SAP session found")
                    return conn.Children(0)

            self.log.info("No existing SAP session found")
            return None

        except Exception:
            self.log.error("Error retrieving existing SAP session", exc_info=True)
            raise

    def open_new_session(self):
        self.log.info("Opening new SAP session")

        try:
            app = self.launcher.get_application()
            conn = app.OpenConnection(self.connection_name, True)
            session = conn.Children(0)
            self.log.info("New SAP session created successfully")
            return session

        except Exception:
            self.log.error("Error opening new SAP connection", exc_info=True)
            raise
