from typing import Tuple
from dotenv import load_dotenv
from helpers.log.logger import logger
import win32com.client, pythoncom, time, os


load_dotenv("config/.env")


class SAP_Launcher:
    def __init__(self):
        self.log = logger("sap")
        self.log.info("Inicializando SAP_Launcher")

        try:
            self.sap_path = os.getenv("SAP_PATH")
            self.log.info(f"SAP_PATH carregado: {self.sap_path}")
        except Exception:
            self.log.error("Erro ao carregar SAP_PATH do .env", exc_info=True)
            raise

    def start(self):
        self.log.info("Iniciando cliente SAP GUI")

        try:
            path = self.sap_path.strip('"')
            shell = win32com.client.Dispatch("WScript.Shell")
            shell.Run(f'"{path}"')
            time.sleep(5)
            self.log.info("SAP GUI iniciado com sucesso")

        except Exception:
            self.log.error("Erro ao iniciar SAP GUI", exc_info=True)
            raise

    def get_application(self):
        self.log.info("Obtendo instância SAPGUI -> Scripting Engine")

        try:
            app = win32com.client.GetObject("SAPGUI").GetScriptingEngine
            return app

        except Exception:
            self.log.error("Erro ao obter SAPGUI ScriptingEngine", exc_info=True)
            raise



class SAP_SessionProvider:
    def __init__(self, launcher: SAP_Launcher):
        self.log = logger("sap")
        self.log.info("Inicializando SAP_SessionProvider")

        self.launcher = launcher
        self.connection_name = os.getenv("SAP_CONNECTION_NAME")

    def get_existing_session(self):
        self.log.info("Procurando sessão SAP já existente")

        try:
            app = self.launcher.get_application()
            if not app:
                self.log.info("Nenhuma instância SAPGUI ativa encontrada")
                return None

            if app.Children.Count > 0:
                conn = app.Children(0)
                if conn.Children.Count > 0:
                    self.log.info("Sessão SAP existente encontrada")
                    return conn.Children(0)

            self.log.info("Nenhuma sessão SAP existente encontrada")
            return None

        except Exception:
            self.log.error("Erro ao tentar recuperar sessão SAP existente", exc_info=True)
            raise

    def open_new_session(self):
        self.log.info("Abrindo nova sessão SAP")

        try:
            app = self.launcher.get_application()
            conn = app.OpenConnection(self.connection_name, True)
            session = conn.Children(0)
            self.log.info("Nova sessão SAP criada com sucesso")
            return session

        except Exception:
            self.log.error("Erro ao abrir nova conexão SAP", exc_info=True)
            raise


class SAP_Authenticator:
    def __init__(self):
        self.log = logger("sap")
        self.log.info("Inicializando SAP_Authenticator")

        self.user = os.getenv("SAP_USER")
        self.password = os.getenv("SAP_PSWD")

    def login(self, session):
        self.log.info("Realizando login no SAP")

        try:
            session.findById("wnd[0]/usr/txtRSYST-BNAME").Text = self.user
            session.findById("wnd[0]/usr/pwdRSYST-BCODE").Text = self.password
            session.findById("wnd[0]").sendVKey(0)

            self.log.info("Login SAP enviado com sucesso")

        except Exception:
            self.log.error("Erro durante login no SAP", exc_info=True)
            raise

            
class SAP_Client:
    def __init__(self, session_provider: SAP_SessionProvider, authenticator: SAP_Authenticator, launcher: SAP_Launcher):
        self.log = logger("sap")
        self.log.info("Inicializando SAP_Client")

        self.session_provider = session_provider
        self.authenticator = authenticator
        self.launcher = launcher
        self.session = None
        self.already_opened = False

    def connect(self):
        self.log.info("Conectando ao SAP...")

        try:
            pythoncom.CoInitialize()

            sess = self.session_provider.get_existing_session()
            if sess:
                self.session = sess
                self.already_opened = True
                self.log.info("Conectado a sessão SAP existente")
                return self.session

            self.log.info("Nenhuma sessão ativa — iniciando nova instância SAP")
            self.launcher.start()
            self.session = self.session_provider.open_new_session()

            self.authenticator.login(self.session)
            self.log.info("Conexão SAP estabelecida com sucesso")

            return self.session

        except Exception:
            self.log.error("Erro ao conectar no SAP", exc_info=True)
            raise

    def run_transaction(self, tcode: str = "/n") -> Tuple[object, bool]:
        self.log.info(f"Executando transação SAP: {tcode}")

        try:
            if not self.session:
                self.connect()

            self.session.findById("wnd[0]/tbar[0]/okcd").Text = tcode
            self.session.findById("wnd[0]").sendVKey(0)

            self.log.info(f"Transação {tcode} executada com sucesso")

            return self.session, self.already_opened

        except Exception:
            self.log.error(f"Erro ao executar transação SAP: {tcode}", exc_info=True)
            raise