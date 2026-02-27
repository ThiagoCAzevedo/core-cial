from __future__ import annotations
from common.logger import logger
from modules.consumption.infrastructure.repository import ConsumptionRepository
import polars as pl


class ConsumptionService:
    def __init__(self, repo: ConsumptionRepository):
        self.log = logger("consumption")
        self.repo = repo
        self.log.info("ConsumptionService initialized")

    def get_values_to_consume(self) -> pl.DataFrame:
        self.log.info("Fetching values to consume")
        df = self.repo.fetch_consumption_frame()
        self.log.info(f"Values retrieved — rows={df.height}")
        return df

    def update_consumption(self, df: pl.DataFrame, batch_size: int) -> int:
        self.log.info("Updating consumption values")
        updated = self.repo.update_consumption(df, batch_size)
        self.log.info(f"Consumption values updated — rows={updated}")
        return updated