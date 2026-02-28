from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert
from modules.requests_builder.domain.models import RequestsMade
from modules.requests_builder.infrastructure.pkmc_adapter import PKMC_Client
from modules.requests_builder.infrastructure.pk05_adapter import PK05_Client
from common.logger import logger
import polars as pl


class QuantityToRequestService:
    """Service to calculate and manage quantities to request"""

    def __init__(self, db: Session):
        self.db = db
        self.log = logger("requests_builder")
        self.pkmc_client = PKMC_Client()
        self.pk05_client = PK05_Client()
        self.log.info("Initializing QuantityToRequestService")

    def define_diference_to_request(self) -> pl.LazyFrame:
        """Calculate quantities to request based on current stock levels"""
        self.log.info("Building query for request difference from external APIs")

        try:
            # Fetch PKMC and PK05 from external APIs
            lf_pkmc = self.pkmc_client.get_all()
            lf_pk05 = self.pk05_client.get_all()
            self.log.info("PKMC and PK05 fetched from external APIs")

            # Join PKMC with PK05 on supply_area
            lf = (
                lf_pkmc
                .join(lf_pk05, on="supply_area", how="inner")
                .filter(
                    (pl.col("lb_balance") <= pl.col("qty_for_restock")) &
                    (pl.col("takt").is_not_null())
                )
                .select([
                    "partnumber",
                    "supply_area",
                    "num_reg_circ",
                    "takt",
                    "rack",
                    "lb_balance",
                    "total_theoretical_qty",
                    "qty_for_restock",
                    "qty_per_box",
                    "qty_max_box",
                ])
            )

            lf = lf.with_columns([
                (pl.col("total_theoretical_qty") - pl.col("lb_balance"))
                    .alias("qty_to_request"),

                ((pl.col("total_theoretical_qty") - pl.col("lb_balance"))
                    / pl.col("qty_per_box"))
                    .floor()
                    .alias("qty_boxes_to_request")
            ])

            lf = lf.select([
                "partnumber",
                "num_reg_circ",
                "supply_area",
                "qty_to_request",
                "qty_boxes_to_request",
                "takt",
                "rack"
            ])

            self.log.info("Request difference calculated successfully")
            return lf

        except Exception:
            self.log.error("Error calculating request difference", exc_info=True)
            raise

    def upsert_to_request(self, batch_size: int = 10000) -> int:
        """Upsert quantity-to-request values into requests_made table"""
        self.log.info(f"Upserting to-request values with batch_size={batch_size}")

        try:
            lf = self.define_diference_to_request()
            df = lf.collect()
            rows = df.to_dicts()
            total = len(rows)

            self.log.info(f"Total records for UPSERT: {total}")

            for i in range(0, total, batch_size):
                batch = rows[i : i + batch_size]
                self.log.info(f"UPSERT batch {i} - {i + len(batch)}")

                stmt = insert(RequestsMade).values(batch)
                on_duplicate = {
                    col.name: stmt.inserted[col.name]
                    for col in RequestsMade.__table__.columns
                    if col.name not in ["id", "created_at", "updated_at", "num_shipment"]
                }
                stmt = stmt.on_duplicate_key_update(on_duplicate)

                self.db.execute(stmt)
                self.db.commit()

            self.log.info("UPSERT operation completed successfully")
            return total

        except Exception:
            self.db.rollback()
            self.log.error("Error executing UPSERT", exc_info=True)
            raise
