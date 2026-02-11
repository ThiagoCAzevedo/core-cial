from helpers.log.logger import logger


class SAPSessionManager:
    session = None
    log = logger("sap_manager")

    @classmethod
    def set_session(cls, sess):
        cls.log.info("Definindo sessão SAP no SessionManager")

        try:
            cls.session = sess
            cls.log.info("Sessão SAP definida com sucesso")

        except Exception:
            cls.log.error("Erro ao definir sessão SAP", exc_info=True)
            raise

    @classmethod
    def get_session(cls):
        cls.log.info("Recuperando sessão SAP no SessionManager")

        try:
            if cls.session is None:
                cls.log.error("Nenhuma sessão SAP foi definida ainda")
            else:
                cls.log.info("Sessão SAP recuperada com sucesso")

            return cls.session

        except Exception:
            cls.log.error("Erro ao recuperar sessão SAP", exc_info=True)
            raise