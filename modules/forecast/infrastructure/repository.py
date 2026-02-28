from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert
from common.logger import logger
import polars as pl


class ForecastRepository:
    """Repository for handling Forecast and FX4PD data persistence"""

    def __init__(self, db: Session):
        self.db = db
        self.log = logger("forecast")
        self.log.info("Initializing ForecastRepository")

    def bulk_upsert_fx4pd(self, lf: pl.LazyFrame | pl.DataFrame, batch_size: int = 10000) -> int:
        """Bulk upsert FX4PD records"""
        self.log.info("Starting UPSERT operation for FX4PD table")

        try:
            if isinstance(lf, pl.LazyFrame):
                df = lf.collect()
            else:
                df = lf

            rows = df.to_dicts()
            total = len(rows)
            self.log.info(f"Total records for UPSERT: {total}")

            from database.models.fx4pd import FX4PD
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

            from database.models.forecast import Forecast
            sql_table = Forecast.__table__

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

    def upsert_table(self, model, records: list[dict], batch_size: int):
        total = 0
        try:
            for start in range(0, len(records), batch_size):
                batch = records[start:start+batch_size]

                stmt = insert(model).values(batch)

                ignore_cols = ["id", "created_at", "updated_at"]

                update_stmt = {
                    col.name: stmt.inserted[col.name]
                    for col in model.__table__.columns
                    if col.name not in ignore_cols
                }

                stmt = stmt.on_duplicate_key_update(update_stmt)
                self.db.execute(stmt)
                self.db.commit()

                total += len(batch)
                self.log.info(f"Upsert batch completed — {len(batch)} rows")

        except Exception:
            self.db.rollback()
            self.log.error("Error in FORECAST upsert", exc_info=True)
            raise

        return total