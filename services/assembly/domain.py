from dotenv import load_dotenv
from helpers.log.logger import logger
import os, requests, urllib3


load_dotenv("config/.env")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AccessAssemblyLineApi:
    def __init__(self):
        self.log = logger("assembly")
        self.log.info("Initializing AccessAssemblyLineApi")
        self.al_url = os.getenv("AL_API_ENDPOINT")

    def get_json_response(self):
        self.log.info(f"Sending GET request to {self.al_url}")

        try:
            response = requests.get(self.al_url, verify=False, timeout=5)
            self.log.info(f"Response received — status code: {response.status_code}")
            response.raise_for_status()

        except requests.exceptions.Timeout:
            self.log.error("Timeout while accessing assembly line API", exc_info=True)
            raise

        except requests.exceptions.RequestException:
            self.log.error("Request error while accessing assembly line API", exc_info=True)
            raise

        try:
            data = response.json()
            self.log.info("API JSON successfully processed")
            return data
        
        except Exception:
            self.log.error("Error converting response to JSON", exc_info=True)
            raise