from sqlalchemy import select
from sqlalchemy.orm import Session
from database.models.assembly import Assembly
from common.logger import logger
import polars as pl


class BuffALService:
    """Service to retrieve and process Assembly Line buffer values"""

    def __init__(self, db: Session):
        self.db = db
        self.log = logger("forecast")
        self.log.info("Initializing BuffALService")

    def return_values_from_db(self) -> pl.LazyFrame:
        """Get assembly line values from database (reception lane only)"""
        self.log.info("Building query to return assembly line values (reception)")

        try:
            stmt = (
                select(
                    Assembly.knr,
                    Assembly.knr_fx4pd,
                    Assembly.model,
                    Assembly.lfdnr_sequence,
                )
                .where(Assembly.lane == "reception")
            )
            self.log.info("SQL query successfully built")

        except Exception:
            self.log.error(
                "Error building SQL query in return_values_from_db",
                exc_info=True
            )
            raise

        try:
            rows = self.db.execute(stmt).mappings().all()
            df = pl.DataFrame(rows)
            lf = df.lazy()

            self.log.info(f"Select completed — records returned: {len(rows)}")
            return lf

        except Exception:
            self.log.error(
                "Error executing SELECT for assembly line",
                exc_info=True
            )
            raise