from common.logger import logger
from config.settings import settings
import polars as pl, httpx


class PK05_Client:
    def __init__(self):
        self.base_url = settings.PK05_URL
        self.log = logger("pk05-client")

    def get_all(self) -> pl.LazyFrame:
        try:
            get_url = f"{self.base_url.rstrip('/')}/response/db"
            self.log.info(f"Fetching PK05 data from {get_url}")
            resp = httpx.get(get_url)
            resp.raise_for_status()
            data = resp.json()
            self.log.info(f"Successfully fetched {len(data)} PK05 records")
            return pl.DataFrame(data).lazy()
        except Exception as e:
            self.log.error(f"Error fetching PK05 from {get_url}", exc_info=True)
            raise e

    def update(self, records: list[dict]) -> dict:
        try:
            update_url = f"{self.base_url.rstrip('/')}/upsert"
            self.log.info(f"Updating {len(records)} PK05 records via {update_url}")
            resp = httpx.post(update_url, json=records, timeout=30)
            resp.raise_for_status()
            result = resp.json()
            self.log.info(f"Successfully updated PK05 records: {result}")
            return result
        except Exception as e:
            self.log.error(f"Error updating PK05 via {update_url}", exc_info=True)
            raise e
