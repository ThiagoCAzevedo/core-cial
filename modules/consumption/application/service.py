from sqlalchemy import select
from sqlalchemy.orm import Session
from modules.forecast.domain.models import Forecast
from modules.assembly.domain.models import Assembly
from modules.external_clients.pkmc_client import PKMC_Client
from common.logger import logger
import polars as pl


class ConsumeValuesService:
    """Service to calculate and apply consumption values from Forecast and Assembly data"""

    def __init__(self, db: Session):
        self.db = db
        self.log = logger("consumption")
        self.pkmc_client = PKMC_Client()
        self.log.info("Initializing ConsumeValuesService")

    def values_to_consume(self) -> pl.DataFrame:
        self.log.info("Building query to retrieve consumption values (Forecast x Assembly x PKMC)")

        try:
            stmt_forecast = select(
                Forecast.partnumber,
                Forecast.takt,
                Forecast.rack,
                Forecast.knr_fx4pd,
                Forecast.qty_usage,
            )
            rows_forecast = self.db.execute(stmt_forecast).mappings().all()
            df_forecast = pl.DataFrame(rows_forecast).lazy()

            stmt_assembly = select(
                Assembly.knr_fx4pd,
                Assembly.takt.label("assembly_takt"),
            )
            rows_assembly = self.db.execute(stmt_assembly).mappings().all()
            df_assembly = pl.DataFrame(rows_assembly).lazy()

            self.log.info(f"Forecast loaded: {len(rows_forecast)} records")
            self.log.info(f"Assembly loaded: {len(rows_assembly)} records")

            lf_pkmc = self.pkmc_client.get_all()
            self.log.info("PKMC fetched from external API")

            lf = (
                df_forecast
                .join(
                    df_assembly,
                    left_on=["knr_fx4pd", "takt"],
                    right_on=["knr_fx4pd", "assembly_takt"],
                    how="inner"
                )
                .join(
                    lf_pkmc,
                    left_on="partnumber",
                    right_on="partnumber",
                    how="inner"
                )
                .select([
                    "partnumber",
                    "takt",
                    "rack",
                    "knr_fx4pd",
                    "qty_usage",
                    "assembly_takt",
                    "lb_balance"
                ])
            )

            self.log.info("Joins completed successfully")

        except Exception:
            self.log.error("Error building join query", exc_info=True)
            raise

        try:
            self.log.info("Calculating updated lb_balance based on qty_usage")

            lf = (
                lf.with_columns(
                    (pl.col("lb_balance") - pl.col("qty_usage").fill_null(0))
                    .alias("lb_balance")
                )
                .select(["partnumber", "lb_balance"])
            )

            self.log.info("Calculation completed — final DataFrame prepared")

        except Exception:
            self.log.error("Error calculating lb_balance in DataFrame", exc_info=True)
            raise

        return lf.collect()

    def update_infos(self, df: pl.DataFrame, batch_size: int = 10000) -> dict:
        """Update PKMC via external API with consumption values
        
        Args:
            df: DataFrame with partnumber and updated lb_balance
            batch_size: Number of records to send per API request
            
        Returns:
            Result from external API
        """
        self.log.info(f"Starting update on PKMC via external API for {df.height} records — batch_size={batch_size}")

        try:
            rows = df.to_dicts()
            total = len(rows)
            self.log.info(f"Total records to update: {total}")

            # Process in batches
            all_results = []
            for i in range(0, total, batch_size):
                batch = rows[i : i + batch_size]
                self.log.info(f"UPDATE batch {i} - {i + len(batch)} via external API")

                result = self.pkmc_client.update(batch)
                all_results.append(result)
                self.log.info(f"Batch update result: {result}")

            self.log.info(f"Update completed successfully — {total} records updated via external API")
            return {
                "status": "success",
                "total_records": total,
                "batches_processed": len(all_results),
                "results": all_results
            }

        except Exception:
            self.log.error("Error updating PKMC via external API", exc_info=True)
            raise

