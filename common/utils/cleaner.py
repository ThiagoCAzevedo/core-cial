from __future__ import annotations
from typing import Any, Dict
import polars as pl
from config.settings import settings
from common.logger import logger


class CleanerBase:
    def __init__(self):
        self.log = logger("cleaner")

    def _load_file(
        self, 
        settings_key: str, 
        rows_to_skip: int = 0, 
        separator: str = ","
    ) -> pl.DataFrame:
        """Load a file using Polars from a settings key path"""
        try:
            file_path = getattr(settings, settings_key)
            self.log.info(f"Loading file from {settings_key}: {file_path}")
            
            df = pl.read_csv(
                file_path,
                skip_rows=rows_to_skip,
                separator=separator
            )
            
            self.log.info(f"File loaded successfully: {df.shape[0]} rows, {df.shape[1]} columns")
            return df
        except Exception as e:
            self.log.error(f"Error loading file from {settings_key}", exc_info=True)
            raise

    def _rename(
        self, 
        df: pl.DataFrame, 
        rename_map: Dict[str, str]
    ) -> pl.DataFrame:
        """Rename DataFrame columns based on mapping"""
        try:
            df = df.rename(rename_map)
            self.log.info(f"Columns renamed successfully: {list(rename_map.values())}")
            return df
        except Exception as e:
            self.log.error(f"Error renaming columns", exc_info=True)
            raise
