from common.logger import logger
from config.settings import settings
import polars as pl, httpx


class PK05_Client:
    def __init__(self):
        self.base_url = settings.PK05_URL
        self.log = logger("pk05-client")

    def get_all(self) -> pl.LazyFrame:
        url = f"{self.base_url}/pk05/all"
        try:
            resp = httpx.get(url, timeout=30)
            resp.raise_for_status()
            return pl.DataFrame(resp.json()).lazy()
        except Exception as e:
            self.log.error("Error fetching PK05", exc_info=True)
            raise e