from services.sap.session_manager import SAPSessionManager
from services.sap.client import SAP_Launcher, SAP_Authenticator, SAP_SessionProvider, SAP_Client
from services.request.requester import QuantityToRequest, LM01_Requester
from helpers.log.logger import logger


class DependenciesInjection:
    log = logger("sap")

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

    @staticmethod
    def get_lm01_requester():
        DependenciesInjection.log.info("Criando LM01_Requester")

        try:
            DependenciesInjection.log.info("Obtendo sessão SAP")
            sap = SAPSessionManager().get_session()

            DependenciesInjection.log.info("Calculando dataframe quantity_to_request")
            df = QuantityToRequest()._define_diference_to_request()

            requester = LM01_Requester(sap, df)
            DependenciesInjection.log.info("LM01_Requester criado com sucesso")

            return requester

        except Exception:
            DependenciesInjection.log.error("Erro ao criar LM01_Requester", exc_info=True)
            raise