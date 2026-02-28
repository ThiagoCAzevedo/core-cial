from common.logger import logger
from config.settings import settings
import win32com.client
import time


class SAP_Launcher:
    def __init__(self):
        self.log = logger("sap_manager")
        self.log.info("Initializing SAP_Launcher")
        self.sap_path = settings.SAP_PATH

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
