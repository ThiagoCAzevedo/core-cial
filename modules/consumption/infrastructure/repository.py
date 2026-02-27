from __future__ import annotations
import polars as pl
from sqlalchemy import update
from sqlalchemy.orm import Session
from common.logger import logger
from modules.consumption.infrastructure.models import Forecast, Assembly, PKMC


class ConsumptionRepository:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("consumption")

    def fetch_consumption_frame(self) -> pl.DataFrame:
        self.log.info("Building SQL query for consumption join")

        stmt = (
            self.db.query(
                Forecast.partnumber,
                Forecast.qty_usage,
                PKMC.lb_balance,
            )
            .join(
                Assembly,
                (Forecast.knr_fx4pd == Assembly.knr_fx4pd)
                & (Forecast.takt == Assembly.takt),
            )
            .join(
                PKMC,
                PKMC.partnumber == Forecast.partnumber,
            )
        )

        self.log.info("Executing SELECT query for consumption data")
        rows = stmt.all()

        self.log.info(f"Rows returned: {len(rows)}")

        if not rows:
            return pl.DataFrame({"partnumber": [], "lb_balance": []})

        df = pl.DataFrame(
            {
                "partnumber": [r.partnumber for r in rows],
                "qty_usage": [r.qty_usage for r in rows],
                "lb_balance": [r.lb_balance for r in rows],
            }
        )

        self.log.info("Applying lb_balance calculation (lb_balance - qty_usage)")
        df = df.with_columns(
            (pl.col("lb_balance") - pl.col("qty_usage").fill_null(0)).alias("lb_balance")
        ).select(["partnumber", "lb_balance"])

        return df


    def update_consumption(self, df: pl.DataFrame, batch_size: int) -> int:
        self.log.info(f"Starting PKMC update — total rows={df.height}, batch_size={batch_size}")

        updated_rows = 0

        df_dicts = df.to_dicts()

        for i in range(0, len(df_dicts), batch_size):
            batch = df_dicts[i : i + batch_size]

            for row in batch:
                stmt = (
                    update(PKMC)
                    .where(PKMC.partnumber == row["partnumber"])
                    .values(lb_balance=row["lb_balance"])
                )
                self.db.execute(stmt)

            self.db.commit()
            updated_rows += len(batch)

            self.log.info(f"Batch updated: {len(batch)} rows")

        self.log.info(f"PKMC update completed — total rows updated={updated_rows}")
        return updated_rows