from services.sap_manager.session_manager import SAPSessionManager
from services.sap_manager.client import SAP_Launcher, SAP_Authenticator, SAP_SessionProvider, SAP_Client
from helpers.log.logger import logger


class DependenciesInjection:
    log = logger("sap_manager")

    @staticmethod
    def get_sap_client():
        DependenciesInjection.log.info("Criando instância de SAP_Client")

        try:
            launcher = SAP_Launcher()
            DependenciesInjection.log.info("SAP_Launcher criado")

            provider = SAP_SessionProvider(launcher)
            DependenciesInjection.log.info("SAP_SessionProvider criado")

            auth = SAP_Authenticator()
            DependenciesInjection.log.info("SAP_Authenticator criado")

            client = SAP_Client(provider, auth, launcher)
            DependenciesInjection.log.info("SAP_Client criado com sucesso")

            return client

        except Exception:
            DependenciesInjection.log.error("Erro ao criar SAP_Client", exc_info=True)
            raise

    @staticmethod
    def get_sap_session():
        DependenciesInjection.log.info("Obtendo classe SAPSessionManager")

        try:
            return SAPSessionManager
        except Exception:
            DependenciesInjection.log.error("Erro ao retornar SAPSessionManager", exc_info=True)
            raise