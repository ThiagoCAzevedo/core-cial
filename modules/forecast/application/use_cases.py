"""Use cases for forecast module"""
from common.logger import logger
import polars as pl
from modules.forecast.infrastructure.repositories import (
    ForecastRepository,
    ExternalDataRepository,
)


class BuildForecastDataUseCase:
    """Use case for building forecast data from multiple sources"""
    
    def __init__(
        self,
        forecast_repo: ForecastRepository,
        external_repo: ExternalDataRepository,
    ):
        self.forecast_repo = forecast_repo
        self.external_repo = external_repo
        self.log = logger("forecast-usecase")
    
    def execute(self) -> pl.LazyFrame:
        """Build forecast data by joining FX4PD, PKMC, and PK05
        
        Returns:
            LazyFrame with joined forecast data
        """
        self.log.info("Starting forecast data building use case")
        
        try:
            df_fx4pd = self.forecast_repo.get_fx4pd_data()
            lf_pkmc = self.external_repo.get_pkmc_data()
            lf_pk05 = self.external_repo.get_pk05_data()
            
            self.log.info("All data sources fetched successfully")
            
            # Join all sources
            lf = (
                lf_pkmc
                .join(lf_pk05, on="supply_area", how="inner")
                .join(df_fx4pd, on="partnumber", how="inner")
                .select([
                    "num_reg_circ",
                    "takt",
                    "rack",
                    "lb_balance",
                    "partnumber",
                    "total_theoretical_qty",
                    "qty_for_restock",
                    "qty_per_box",
                    "qty_max_box",
                    "knr_fx4pd",
                    "qty_usage",
                    "qty_unit",
                ])
                .filter(pl.col("lb_balance") <= pl.col("qty_for_restock"))
            )

            self.log.info("Forecast data built successfully")
            return lf
            
        except Exception as e:
            self.log.error(f"Error building forecast data: {str(e)}", exc_info=True)
            raise
