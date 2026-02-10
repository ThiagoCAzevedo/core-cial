from dotenv import load_dotenv
from helpers.log.logger import logger
import os, requests, urllib3


load_dotenv("config/.env")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AccessAssemblyLineApi:
    def __init__(self):
        self.log = logger("assembly")
        self.log.info("Inicializando AccessAssemblyLineApi")

        try:
            self.al_url = os.getenv("AL_API_ENDPOINT")
            self.log.info(f"Endpoint carregado: {self.al_url}")

        except Exception:
            self.log.error("Erro ao carregar variável AL_API_ENDPOINT", exc_info=True)
            raise


    def get_json_response(self):
        self.log.info(f"Realizando requisição GET para {self.al_url}")

        try:
            response = requests.get(self.al_url, verify=False, timeout=5)
            self.log.info(f"Resposta recebida — status code: {response.status_code}")

            response.raise_for_status()

        except requests.exceptions.Timeout:
            self.log.error("Timeout ao acessar API da linha de montagem", exc_info=True)
            raise

        except requests.exceptions.RequestException:
            self.log.error("Erro de requisição ao acessar API da linha de montagem", exc_info=True)
            raise

        try:
            data = response.json()
            self.log.info("JSON da API processado com sucesso")
            return data
        
        except Exception:
            self.log.error("Erro ao converter resposta em JSON", exc_info=True)
            raise