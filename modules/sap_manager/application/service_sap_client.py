from typing import Tuple
from dotenv import load_dotenv
from common.logger import logger
import win32com.client
import time
import os


load_dotenv("config/.env")


class SAP_Launcher:
    """Handles starting and connecting to SAP GUI"""

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
        """Start SAP GUI client"""
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
        """Retrieve SAPGUI scripting engine"""
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
    """Manages SAP session creation and retrieval"""

    def __init__(self):
        self.log = logger("sap_manager")
        self.log.info("Initializing SAP_SessionProvider")
        self.launcher = SAP_Launcher()
        self.connection_name = os.getenv("SAP_CONNECTION_NAME")

    def get_existing_session(self):
        """Find active SAP session"""
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
        """Create new SAP session"""
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


class SAP_Client:
    """Main SAP client for managing connections"""

    def __init__(self):
        self.log = logger("sap_manager")
        self.log.info("Initializing SAP_Client")
        self.provider = SAP_SessionProvider()
        self.session = None

    def connect(self):
        """Connect to SAP and retrieve or create session"""
        self.log.info("Starting SAP connection process")
        try:
            self.session = self.provider.get_existing_session()
            if not self.session:
                self.log.info("No existing session, opening new one")
                self.session = self.provider.open_new_session()
            self.log.info("SAP connection established")
            return self.session
        except Exception:
            self.log.error("Error establishing SAP connection", exc_info=True)
            raise

    def run_transaction(self, transaction: str) -> Tuple:
        """Run transaction in SAP"""
        if not self.session:
            raise Exception("No SAP session available")

        self.log.info(f"Running SAP transaction: {transaction}")
        try:
            self.session.StartTransaction(None, None, transaction, False)
            self.log.info(f"Transaction {transaction} started")
            return self.session, None
        except Exception:
            self.log.error(f"Error running transaction {transaction}", exc_info=True)
            raise
