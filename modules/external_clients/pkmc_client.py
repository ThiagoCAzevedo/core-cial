from common.logger import logger
from config.settings import settings
import polars as pl, httpx


class PKMC_Client:
    def __init__(self):
        self.base_url = settings.PKMC_URL
        self.log = logger("pkmc-client")

    def get_all(self) -> pl.LazyFrame:
        try:
            get_url = f"{self.base_url.rstrip('/')}/db"
            self.log.info(f"Fetching PKMC data from {get_url}")
            resp = httpx.get(get_url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            self.log.info(f"Successfully fetched {len(data)} PKMC records")
            return pl.DataFrame(data).lazy()
        except Exception as e:
            self.log.error(f"Error fetching PKMC from {get_url}", exc_info=True)
            raise e

    def update(self, records: list[dict]) -> dict:
        try:
            update_url = f"{self.base_url.rstrip('/')}/update"
            self.log.info(f"Updating {len(records)} PKMC records via {update_url}")
            resp = httpx.post(update_url, json=records, timeout=30)
            resp.raise_for_status()
            result = resp.json()
            self.log.info(f"Successfully updated PKMC records: {result}")
            return result
        except Exception as e:
            self.log.error(f"Error updating PKMC via {update_url}", exc_info=True)
            raise e