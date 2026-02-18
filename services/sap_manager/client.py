from typing import Tuple
from dotenv import load_dotenv
from helpers.log.logger import logger
import win32com.client, pythoncom, time, os


load_dotenv("config/.env")


class SAP_Launcher:
    def __init__(self):
        self.log = logger("sap_manager")
        self.log.info("Initializing SAP_Launcher")

        try:
            self.sap_path = os.getenv("SAP_PATH")
            self.log.info(f"SAP_PATH loaded: {self.sap_path}")
        except Exception:
            self.log.error("Error loading SAP_PATH from .env", exc_info=True)
            raise

    def start(self):
        self.log.info("Starting SAP GUI client")

        try:
            path = self.sap_path.strip('"')
            shell = win32com.client.Dispatch("WScript.Shell")
            shell.Run(f'"{path}"')
            time.sleep(5)
            self.log.info("SAP GUI started successfully")

        except Exception:
            self.log.error("Error starting SAP GUI", exc_info=True)
            raise

    def get_application(self):
        self.log.info("Retrieving SAPGUI → Scripting Engine instance")

        try:
            for i in range(20):
                try:
                    app = win32com.client.GetObject("SAPGUI").GetScriptingEngine
                    return app
                except:
                    time.sleep(0.5)
        except Exception:
            self.log.error("Error getting SAPGUI ScriptingEngine", exc_info=True)
            raise


class SAP_SessionProvider:
    def __init__(self):
        self.log = logger("sap_manager")
        self.log.info("Initializing SAP_SessionProvider")

        self.launcher = SAP_Launcher()
        self.connection_name = os.getenv("SAP_CONNECTION_NAME")

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


class SAP_Authenticator:
    def __init__(self):
        self.log = logger("sap_manager")
        self.log.info("Initializing SAP_Authenticator")

        self.user = os.getenv("SAP_USER")
        self.password = os.getenv("SAP_PSWD")

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