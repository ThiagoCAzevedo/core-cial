from sqlalchemy.orm import Session
from sqlalchemy import insert
from common.logger import logger
import polars as pl


class ForecastRepository:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("forecast")

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