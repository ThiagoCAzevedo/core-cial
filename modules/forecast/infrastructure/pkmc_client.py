from common.logger import logger
from config.settings import settings
import polars as pl, httpx


class PKMC_Client:
    def __init__(self):
        self.base_url = settings.PKMC_URL
        self.log = logger("pkmc-client")

    def get_all(self) -> pl.LazyFrame:
        try:
            get_url = f"{self.base_url.rstrip('/')}/response/db"
            self.log.info(f"Fetching PKMC data from {get_url}")

            resp = httpx.get(get_url, timeout=30)
            resp.raise_for_status()

            payload = resp.json()

            if isinstance(payload, dict) and "data" in payload:
                records = payload["data"]
            elif isinstance(payload, list):
                records = payload
            else:
                raise ValueError(
                    f"Invalid PKMC response, expected list or dict['data'], got: {payload}"
                )

            if not isinstance(records, list):
                raise ValueError(
                    f"Invalid PKMC response 'data' field, expected list, got: {records}"
                )

            self.log.info(f"Successfully fetched {len(records)} PKMC records")
            return pl.DataFrame(records).lazy()

        except Exception as e:
            self.log.error(f"Error fetching PKMC from {get_url}", exc_info=True)
            raise e