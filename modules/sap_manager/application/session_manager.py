from common.logger import logger


class SAPSessionManager:
    """Class-level session manager for SAP connection"""

    session = None
    log = logger("sap_manager")

    @classmethod
    def set_session(cls, sess):
        """Store SAP session for reuse"""
        cls.log.info("Setting SAP session in SessionManager")
        try:
            cls.session = sess
            cls.log.info("SAP session set successfully")
        except Exception:
            cls.log.error("Error setting SAP session", exc_info=True)
            raise

    @classmethod
    def get_session(cls):
        """Retrieve stored SAP session"""
        cls.log.info("Retrieving SAP session from SessionManager")
        try:
            if cls.session is None:
                cls.log.warning("No SAP session has been set yet")
            else:
                cls.log.info("SAP session retrieved successfully")
            return cls.session
        except Exception:
            cls.log.error("Error retrieving SAP session", exc_info=True)
            raise
