from common.logger import logger
import polars as pl, httpx


class PKMC_Client:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.log = logger("pkmc-client")

    def get_all(self) -> pl.LazyFrame:
        url = f"{self.base_url}/pkmc/all"
        try:
            resp = httpx.get(url, timeout=30)
            resp.raise_for_status()
            return pl.DataFrame(resp.json()).lazy()
        except Exception as e:
            self.log.error("Error fetching PKMC", exc_info=True)
            raise e