from __future__ import annotations
from typing import Dict, Any
import requests
import urllib3
from common.logger import logger
from config.settings import settings


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AssemblyApiClient:
    def __init__(self, session: requests.Session | None = None):
        self.log = logger("assembly")
        self.session = session or requests.Session()
        self.url = settings.AL_API_ENDPOINT

        self.log.debug(f"AssemblyApiClient initialized — endpoint={self.url}")

    def get_json(self) -> Dict[str, Any]:
        self.log.info(f"Requesting Assembly Line API: {self.url}")

        try:
            response = self.session.get(
                self.url,
                verify=False,
                timeout=5,
            )
            response.raise_for_status()
        except requests.Timeout as e:
            msg = "Timeout accessing Assembly Line API"
            self.log.error(msg)
            raise msg from e
        except requests.RequestException as e:
            msg = f"Error requesting Assembly Line API: {e}"
            self.log.error(msg)
            raise msg from e

        try:
            data = response.json()
            self.log.info("JSON successfully received")
            return data
        except ValueError as e:
            msg = "Error converting to JSON"
            self.log.error(msg)
            raise msg from e