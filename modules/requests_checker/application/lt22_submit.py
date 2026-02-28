from common.logger import logger


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
