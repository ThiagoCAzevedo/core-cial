from services.sap_manager.session_manager import SAPSessionManager
from services.sap_manager.client import SAP_Launcher, SAP_Authenticator, SAP_SessionProvider, SAP_Client
from helpers.log.logger import logger


class DependenciesInjection:
    log = logger("sap_manager")

    @staticmethod
    def get_sap_client():
        DependenciesInjection.log.info("Creating SAP_Client instance")

        try:
            launcher = SAP_Launcher()
            DependenciesInjection.log.info("SAP_Launcher created")

            provider = SAP_SessionProvider(launcher)
            DependenciesInjection.log.info("SAP_SessionProvider created")

            auth = SAP_Authenticator()
            DependenciesInjection.log.info("SAP_Authenticator created")

            client = SAP_Client(provider, auth, launcher)
            DependenciesInjection.log.info("SAP_Client created successfully")

            return client

        except Exception:
            DependenciesInjection.log.error("Error creating SAP_Client", exc_info=True)
            raise

    @staticmethod
    def get_sap_session():
        DependenciesInjection.log.info("Retrieving SAPSessionManager class")

        try:
            return SAPSessionManager
        except Exception:
            DependenciesInjection.log.error("Error returning SAPSessionManager", exc_info=True)
            raise