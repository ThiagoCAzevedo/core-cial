from services.sap.session_manager import SAPSessionManager
from services.sap.client import SAP_Launcher, SAP_Authenticator, SAP_SessionProvider, SAP_Client
from services.request.requester import QuantityToRequest, LM01_Requester


class DependenciesInjection:
    @staticmethod
    def get_sap_client():
        launcher = SAP_Launcher()
        provider = SAP_SessionProvider(launcher)
        auth = SAP_Authenticator()
        return SAP_Client(provider, auth, launcher)

    @staticmethod
    def get_sap_session():
        return SAPSessionManager

    @staticmethod
    def get_lm01_requester():
        sap = SAPSessionManager().get_session()
        df = QuantityToRequest()._define_diference_to_request()
        return LM01_Requester(sap, df)