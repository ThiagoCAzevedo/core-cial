"""Use cases for requests_builder module"""
from common.logger import logger
import polars as pl
from modules.requests_builder.domain.entities import OrderRequest
from modules.requests_builder.infrastructure.repositories import (
    RequestsRepository,
    ExternalDataRepository,
)


class CalculateOrderQuantitiesUseCase:
    """Use case for calculating order quantities"""
    
    def __init__(self, external_repo: ExternalDataRepository):
        self.external_repo = external_repo
        self.log = logger("requests-builder-usecase")
    
    def execute(self) -> pl.LazyFrame:
        """Calculate quantities to request from external sources
        
        Returns:
            LazyFrame with calculated order quantities
        """
        self.log.info("Starting order quantities calculation use case")
        
        try:
            lf_pkmc = self.external_repo.get_pkmc_data()
            lf_pk05 = self.external_repo.get_pk05_data()
            
            self.log.info("External data fetched successfully")
            
            # Join and calculate quantities
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
            
            # Calculate quantities
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
            
            self.log.info("Order quantities calculated successfully")
            return lf
            
        except Exception as e:
            self.log.error(f"Error calculating order quantities: {str(e)}", exc_info=True)
            raise


class SaveOrderRequestsUseCase:
    """Use case for saving order requests to database"""
    
    def __init__(self, requests_repo: RequestsRepository):
        self.requests_repo = requests_repo
        self.log = logger("requests-save-usecase")
    
    def execute(self, lf: pl.LazyFrame, batch_size: int = 10000) -> int:
        """Save calculated orders to database
        
        Args:
            lf: LazyFrame with order data
            batch_size: Number of records per batch
            
        Returns:
            Total number of records saved
        """
        self.log.info("Starting order requests save use case")
        
        try:
            df = lf.collect()
            rows = df.to_dicts()
            total = len(rows)
            
            self.log.info(f"Saving {total} order requests in batches of {batch_size}")
            
            for i in range(0, total, batch_size):
                batch = rows[i : i + batch_size]
                self.requests_repo.upsert_requests(batch)
                self.log.info(f"Saved batch {i} - {i + len(batch)}")
            
            self.log.info(f"Successfully saved {total} order requests")
            return total
            
        except Exception as e:
            self.log.error(f"Error saving order requests: {str(e)}", exc_info=True)
            raise
