from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert
from common.logger import logger
from modules.forecast.domain.models import FX4PD
import polars as pl


class ForecastRepository:
    """Repository for handling Forecast and FX4PD data persistence"""

    def __init__(self, db: Session):
        self.db = db
        self.log = logger("forecast")
        self.log.info("Initializing ForecastRepository")

    def bulk_upsert_fx4pd(self, lf: pl.LazyFrame | pl.DataFrame, batch_size: int = 10000) -> int:
        self.log.info("Starting UPSERT operation for FX4PD table")

        try:
            if isinstance(lf, pl.LazyFrame):
                df = lf.collect()
            else:
                df = lf

            rows = df.to_dicts()
            total = len(rows)
            self.log.info(f"Total records for UPSERT: {total}")

            sql_table = FX4PD.__table__

            for i in range(0, total, batch_size):
                batch = rows[i : i + batch_size]
                self.log.info(f"UPSERT batch {i} - {i + len(batch)}")

                stmt = insert(sql_table).values(batch)
                on_duplicate = {
                    col.name: stmt.inserted[col.name]
                    for col in sql_table.columns
                    if col.name not in ["created_at", "updated_at"]
                }
                stmt = stmt.on_duplicate_key_update(on_duplicate)

                self.db.execute(stmt)
                self.db.commit()

            self.log.info("UPSERT operation for FX4PD completed successfully")
            return total

        except Exception:
            self.db.rollback()
            self.log.error("Error executing UPSERT for FX4PD", exc_info=True)
            raise

    def bulk_upsert_forecast(self, lf: pl.LazyFrame | pl.DataFrame, batch_size: int = 10000) -> int:
        """Bulk upsert Forecast records"""
        self.log.info("Starting UPSERT operation for Forecast table")

        try:
            if isinstance(lf, pl.LazyFrame):
                df = lf.collect()
            else:
                df = lf

            rows = df.to_dicts()
            total = len(rows)
            self.log.info(f"Total records for UPSERT: {total}")

            from modules.forecast.domain.models import Forecast
            sql_table = Forecast.__table__

            for i in range(0, total, batch_size):
                batch = rows[i : i + batch_size]
                self.log.info(f"UPSERT batch {i} - {i + len(batch)}")

                stmt = insert(sql_table).values(batch)
                on_duplicate = {
                    col.name: stmt.inserted[col.name]
                    for col in sql_table.columns
                    if col.name not in ["id", "created_at", "updated_at"]
                }
                stmt = stmt.on_duplicate_key_update(on_duplicate)

                self.db.execute(stmt)
                self.db.commit()

            self.log.info("UPSERT operation for Forecast completed successfully")
            return total

        except Exception:
            self.db.rollback()
            self.log.error("Error executing UPSERT for Forecast", exc_info=True)
            raise

    def select_fx4pd_buffer(self, stmt):
        self.log.info("Executing FX4PD SELECT query")
        rows = self.db.execute(stmt).all()
        return pl.DataFrame(rows)

    def select_join_forecast(self, stmt):
        self.log.info("Executing FORECAST JOIN query")
        rows = self.db.execute(stmt).all()
        return pl.DataFrame(rows)