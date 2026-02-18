from pathlib import Path
from helpers.log.logger import logger
import os


class SP02_Session:
    def __init__(self, sap):
        self.log = logger("sap")
        self.log.info("Initializing SP02_Session")

        self.sap = sap

    def open(self):
        self.log.info("Opening SAP transaction /nSP02")

        try:
            session, _ = self.sap.run_transaction("/nSP02")
            self.log.info("SP02 session opened successfully")
            return session

        except Exception:
            self.log.error("Error opening SAP transaction /nSP02", exc_info=True)
            raise


class SP02_Rows:
    def __init__(self, session):
        self.log = logger("sap")
        self.log.info("Initializing SP02_Rows")

        self.session = session

    def exists(self, element_id: str) -> bool:
        try:
            self.session.findById(element_id)
            return True
        except Exception:
            return False

    def text(self, element_id: str) -> str:
        try:
            if self.exists(element_id):
                return self.session.findById(element_id).Text
            return ""
        except Exception:
            self.log.error(f"Error retrieving text from element {element_id}", exc_info=True)
            return ""

    def first_valid_row(self) -> int:
        self.log.info("Searching for first valid row in SP02")

        try:
            i = 0
            while True:
                el = f"wnd[0]/usr/lbl[51,{i}]"
                if self.exists(el) and self.text(el).strip() != "":
                    self.log.info(f"First valid row found: {i}")
                    return i
                i += 1

        except Exception:
            self.log.error("Error searching for first valid row in SP02", exc_info=True)
            raise

    def iter_rows(self, start: int):
        self.log.info(f"Iterating SP02 rows starting from {start}")

        try:
            i = start
            while True:
                el = f"wnd[0]/usr/lbl[51,{i}]"
                if not self.exists(el):
                    break
                yield i
                i += 1

        except Exception:
            self.log.error("Error iterating rows in SP02", exc_info=True)
            raise

    def find_lt22_job(self):
        self.log.info("Searching for LT22 job inside SP02 listing")

        try:
            start = self.first_valid_row()
            for i in self.iter_rows(start):
                name = self.text(f"wnd[0]/usr/lbl[51,{i}]").lower()
                hour = self.text(f"wnd[0]/usr/lbl[30,{i}]")

                if "lt22" in name:
                    self.log.info(f"LT22 job found on row {i}")
                    return {"index": i, "name": name, "hour": hour}

            self.log.info("No LT22 job found in SP02")
            return None

        except Exception:
            self.log.error("Error searching for LT22 job in SP02", exc_info=True)
            raise


class SP02_Actions:
    def __init__(self, sap):
        self.log = logger("sap")
        self.log.info("Initializing SP02_Actions")

        self.sap = sap
        self.path = Path(os.getenv("SAP_PATH")).resolve()
        self.filename = "alf_lt22"

    def download(self, session, index: int):
        self.log.info(f"Starting LT22 job download — row {index}")

        try:
            session.findById(f"wnd[0]/usr/chk[1,{index}]").Selected = False
            session.findById(f"wnd[0]/usr/lbl[14,{index}]").setFocus()
            session.findById("wnd[0]").sendVKey(2)
            session.findById("wnd[0]/tbar[1]/btn[48]").press()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[1]/usr/ctxtDY_PATH").Text = str(self.path)
            session.findById("wnd[1]/usr/ctxtDY_FILENAME").Text = str(self.filename)
            session.findById("wnd[1]/tbar[0]/btn[11]").press()

            self.log.info(f"Download completed and saved to: {self.path}/{self.filename}")

        except Exception:
            self.log.error(f"Error downloading job at row {index}", exc_info=True)
            raise

    def clean(self, index: int):
        self.log.info(f"Cleaning LT22 job at row {index}")

        try:
            session, _ = self.sap.run_transaction("/nsp02")
            session.findById(f"wnd[0]/usr/chk[1,{index}]").Selected = True
            session.findById("wnd[0]/tbar[1]/btn[14]").press()
            session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()

            self.log.info(f"LT22 job successfully removed at row {index}")

        except Exception:
            self.log.error(f"Error cleaning LT22 job at row {index}", exc_info=True)
            raise