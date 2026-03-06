"""Use cases for consumption module - encapsulates business logic"""
from common.logger import logger
import polars as pl
from modules.consumption.domain.entities import ConsumeValue
from modules.consumption.infrastructure.repositories import (
    ConsumptionRepository,
    ExternalDataRepository,
)


class CalculateConsumptionUseCase:
    """Use case for calculating consumption values"""
    
    def __init__(
        self,
        consumption_repo: ConsumptionRepository,
        external_repo: ExternalDataRepository,
    ):
        self.consumption_repo = consumption_repo
        self.external_repo = external_repo
        self.log = logger("consumption-usecase")
    
    def execute(self) -> pl.DataFrame:
        self.log.info("Starting consumption calculation use case")
        
        try:
            df_forecast = self.consumption_repo.get_forecast_data()
            df_assembly = self.consumption_repo.get_assembly_data()
            lf_pkmc = self.external_repo.get_pkmc_data()
            
            self.log.info("All data sources fetched successfully")

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
                    on="partnumber",
                    how="inner",
                    suffix="_pkmc"
                )
            )
            
            self.log.info("Joins completed successfully")
            
            lf = (
                lf.with_columns(
                    (pl.col("lb_balance") - pl.col("qty_usage").fill_null(0))
                    .alias("lb_balance")
                )
                .select(["partnumber", "lb_balance"])
            )
            
            self.log.info("Calculation completed")
            return lf.collect()
            
        except Exception as e:
            self.log.error(f"Error in consumption calculation: {str(e)}", exc_info=True)
            raise


class UpdateConsumptionUseCase:
    """Use case for updating consumption data via external API"""
    
    def __init__(self, external_repo: ExternalDataRepository):
        self.external_repo = external_repo
        self.log = logger("consumption-update-usecase")
    
    def execute(self, df: pl.DataFrame, batch_size: int = 10000) -> dict:
        """Update PKMC with consumption values
        
        Args:
            df: DataFrame with partnumber and updated lb_balance
            batch_size: Number of records per batch
            
        Returns:
            Result from update operation
        """
        self.log.info(
            f"Starting consumption update use case for {df.height} records"
        )
        
        try:
            rows = df.to_dicts()
            total = len(rows)
            
            all_results = []
            for i in range(0, total, batch_size):
                batch = rows[i : i + batch_size]
                self.log.info(f"Updating batch {i} - {i + len(batch)}")
                
                result = self.external_repo.update_pkmc_data(batch)
                all_results.append(result)
            
            self.log.info(f"Successfully updated {total} records")
            return {
                "status": "success",
                "total_records": total,
                "batches_processed": len(all_results),
                "results": all_results
            }
            
        except Exception as e:
            self.log.error(f"Error updating consumption data: {str(e)}", exc_info=True)
            raise
